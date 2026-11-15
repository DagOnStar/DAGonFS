//
// Created by frank on 1/14/25.
//

#include "DistributedCode.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <vector>

#include <fcntl.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <mpi.h>

#include "mpi_data.hpp"
#include "RequestSender.hpp"
#include "../blocks/Blocks.hpp"
#include "../ramfs/FileSystem.hpp"

extern char **environ;

using namespace std;

int DistributedCode::mpiRank = -1;
int DistributedCode::mpiWorldSize = 0;
string DistributedCode::fsPath = string();
DistributedCode *DistributedCode::instance = nullptr;
DataBlockManager *DistributedCode::dataBlockManager = nullptr;
double DistributedCode::lastWriteTime = 0.0;
double DistributedCode::lastReadTime = 0.0;
double DistributedCode::DAGonFSWriteSGElapsedTime = 0.0;
double DistributedCode::DAGonFSReadSGElapsedTime = 0.0;

DistributedCode *DistributedCode::getInstance(int rank, int worldSize, const char *mountpointPath) {
	if (instance == nullptr) {
		instance = new DistributedCode(rank, worldSize, mountpointPath);
	}
	return instance;
}

DistributedCode::DistributedCode(int rank, int worldSize, const char* mountpointPath) {
	mpiRank = rank;
	mpiWorldSize = worldSize;
	fsPath = mountpointPath;
	fsPath += "/" + to_string(mpiRank);
	dataBlockManager = DataBlockManager::getInstance(mpiWorldSize);
}

void DistributedCode::setup() {
}

string DistributedCode::localPath(const string &relativePath) {
	return fsPath + relativePath;
}

void DistributedCode::start() {
	vector<char> message;
	bool running = true;
	while (running) {
		// One self-describing message per request: probe for its size first.
		MPI_Status status;
		MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
		int count = 0;
		MPI_Get_count(&status, MPI_BYTE, &count);
		const int source = status.MPI_SOURCE;
		message.resize(static_cast<size_t>(count));
		MPI_Recv(message.data(), count, MPI_BYTE, source, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		PeerRequest request{};
		if (static_cast<size_t>(count) < sizeof(PeerRequest)) {
			cerr << "Process " << mpiRank << " - ignoring malformed request from " << source << endl;
			continue;
		}
		memcpy(&request, message.data(), sizeof(PeerRequest));
		const size_t firstLength = request.firstPathLength;
		const size_t secondLength = request.secondPathLength;
		if (firstLength >= DAGONFS_MAX_REQUEST_PATH || secondLength >= DAGONFS_MAX_REQUEST_PATH ||
		    sizeof(PeerRequest) + firstLength + secondLength != static_cast<size_t>(count)) {
			cerr << "Process " << mpiRank << " - ignoring malformed request from " << source << endl;
			continue;
		}
		const string first(message.data() + sizeof(PeerRequest), firstLength);
		const string second(message.data() + sizeof(PeerRequest) + firstLength, secondLength);

		if (source == mpiRank) {
			// Only termination is ever addressed to the sending peer itself.
			if (request.type == TERMINATE) {
				running = false;
			}
			continue;
		}
		switch (request.type) {
		case WRITE:
			DAGonFS_Write(source, nullptr, request.io.inode, request.io.fileSize);
			break;
		case READ:
			DAGonFS_Read(source, request.io.inode, request.io.fileSize, request.io.reqSize, request.io.offset);
			break;
		case CREATE_FILE:
			createFile(request, first, second);
			break;
		case DELETE_FILE:
			deleteFile(request, first, second);
			break;
		case CREATE_DIR:
			createDir(request, first, second);
			break;
		case DELETE_DIR:
			deleteDir(request, first, second);
			break;
		case RENAME:
			renameHandler(request, first, second);
			break;
		case LINK:
			linkHandler(request, first, second);
			break;
		case TERMINATE:
			running = false;
			unmountFileSystem();
			break;
		default:
			break;
		}
	}
}

void DistributedCode::unmountFileSystem() {
	FileSystem::unmountFromThread = true;
#ifdef __APPLE__
	const char *tool = "umount";
	char *const argv[] = {const_cast<char *>("umount"), const_cast<char *>(fsPath.c_str()), nullptr};
#else
	const char *tool = "fusermount3";
	char *const argv[] = {const_cast<char *>("fusermount3"), const_cast<char *>("-u"), const_cast<char *>(fsPath.c_str()), nullptr};
#endif
	pid_t child;
	if (posix_spawnp(&child, tool, nullptr, nullptr, argv, environ) == 0) {
		int status = 0;
		waitpid(child, &status, 0);
	} else {
		cerr << "Process " << mpiRank << " - cannot run " << tool << " to unmount " << fsPath << endl;
	}
}

void DistributedCode::DAGonFS_Write(int sourceRank, void *buffer, fuse_ino_t inode, size_t fileSize) {
	double startWrite = MPI_Wtime();
	if (mpiRank == sourceRank) {
		PeerRequest request{};
		request.type = WRITE;
		request.io.inode = inode;
		request.io.fileSize = fileSize;
		request.io.reqSize = fileSize;
		RequestSender::send(request, string(), string(), mpiRank, mpiWorldSize);
	}

	const double elapsed = dataBlockManager->scatterStore(buffer, inode, fileSize, mpiRank, sourceRank);

	if (mpiRank != sourceRank) {
		// Mirror the new size in the local replica of the file.
		File *file = FileSystem::FileForGlobalId(inode);
		if (file != nullptr) {
			file->m_fuseEntryParam.attr.st_size = static_cast<off_t>(fileSize);
			file->m_fuseEntryParam.attr.st_blocks = static_cast<blkcnt_t>((fileSize + 511) / 512);
			file->m_storedSize = fileSize;
			FileSystem::InvalidateINode(file->inodeNumber);
		}
	} else {
		DAGonFSWriteSGElapsedTime = elapsed;
		lastWriteTime = MPI_Wtime() - startWrite;
	}
}

void* DistributedCode::DAGonFS_Read(int sourceRank, fuse_ino_t inode, size_t fileSize, size_t reqSize, off_t offset) {
	double startRead = MPI_Wtime();
	if (mpiRank == sourceRank) {
		PeerRequest request{};
		request.type = READ;
		request.io.inode = inode;
		request.io.fileSize = fileSize;
		request.io.reqSize = reqSize;
		request.io.offset = offset;
		RequestSender::send(request, string(), string(), mpiRank, mpiWorldSize);
	}

	double elapsed = 0.0;
	void *buffer = dataBlockManager->gatherLoad(inode, fileSize, mpiRank, sourceRank, &elapsed);
	if (mpiRank == sourceRank) {
		DAGonFSReadSGElapsedTime = elapsed;
		lastReadTime = MPI_Wtime() - startRead;
	}
	return buffer;
}

void DistributedCode::createFile(const PeerRequest &request, const string &first, const string &) {
	FileSystem::pendingGlobalId = request.globalId;
	FileSystem::createFileFromThread = true;
	const string absPath = localPath(first);
	int fd = open(absPath.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0666);
	if (fd >= 0) {
		close(fd);
	} else {
		FileSystem::createFileFromThread = false;
		FileSystem::pendingGlobalId = 0;
		cerr << "Process " << mpiRank << " - replicated create of " << absPath << " failed: " << strerror(errno) << endl;
	}
}

void DistributedCode::deleteFile(const PeerRequest &, const string &first, const string &) {
	FileSystem::deleteFileFromThread = true;
	if (unlink(localPath(first).c_str()) != 0) {
		FileSystem::deleteFileFromThread = false;
	}
}

void DistributedCode::createDir(const PeerRequest &, const string &first, const string &) {
	FileSystem::createDirFromThread = true;
	if (mkdir(localPath(first).c_str(), 0777) != 0) {
		FileSystem::createDirFromThread = false;
	}
}

void DistributedCode::deleteDir(const PeerRequest &, const string &first, const string &) {
	FileSystem::deleteDirFromThread = true;
	if (rmdir(localPath(first).c_str()) != 0) {
		FileSystem::deleteDirFromThread = false;
	}
}

void DistributedCode::renameHandler(const PeerRequest &, const string &first, const string &second) {
	FileSystem::renameFromThread = true;
	if (rename(localPath(first).c_str(), localPath(second).c_str()) != 0) {
		FileSystem::renameFromThread = false;
	}
}

void DistributedCode::linkHandler(const PeerRequest &, const string &first, const string &second) {
	FileSystem::linkFromThread = true;
	if (link(localPath(first).c_str(), localPath(second).c_str()) != 0) {
		FileSystem::linkFromThread = false;
	}
}
