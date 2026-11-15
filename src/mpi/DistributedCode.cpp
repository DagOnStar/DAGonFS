//
// Created by frank on 1/14/25.
//

#include "DistributedCode.hpp"

#include <algorithm>
#include <cerrno>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <chrono>
#include <thread>
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

DistributedCode *DistributedCode::getInstance() {
	if (instance == nullptr) {
		instance = new DistributedCode();
	}
	return instance;
}

void DistributedCode::configure(int rank, int worldSize, const string &replayMountPoint) {
	mpiRank = rank;
	mpiWorldSize = worldSize;
	fsPath = replayMountPoint;
	dataBlockManager = DataBlockManager::getInstance(mpiWorldSize);
}

string DistributedCode::localPath(const string &relativePath) {
	return fsPath + relativePath;
}

void DistributedCode::waitForRequest(MPI_Status &status) {
	// A blocking MPI_Probe would hold the MPI library's progress lock for as
	// long as it waits; with MPI_THREAD_MULTIPLE this starves the FUSE thread's
	// own MPI calls (measured: ~400 ms per replicated operation across nodes).
	// Poll instead, backing off to at most kMaxIdleSleep while idle.
	constexpr auto kMaxIdleSleep = std::chrono::microseconds(500);
	auto idle = std::chrono::microseconds(0);
	for (;;) {
		int arrived = 0;
		MPI_Iprobe(MPI_ANY_SOURCE, DAGONFS_TAG_REQUEST, MPI_COMM_WORLD, &arrived, &status);
		if (arrived) {
			return;
		}
		if (idle.count() == 0) {
			std::this_thread::yield();
			idle = std::chrono::microseconds(1);
		} else {
			std::this_thread::sleep_for(idle);
			idle = std::min(idle * 2, std::chrono::duration_cast<std::chrono::microseconds>(kMaxIdleSleep));
		}
	}
}

namespace {

/** A namespace operation received from another peer, replayed in arrival order. */
struct Replay {
	PeerRequest request;
	string first;
	string second;
	bool stop;
};

std::mutex replayMutex;
std::condition_variable replayReady;
std::deque<Replay> replayQueue;

void enqueue(Replay replay) {
	{
		std::lock_guard<std::mutex> lock(replayMutex);
		replayQueue.push_back(std::move(replay));
	}
	replayReady.notify_one();
}

} // namespace

void DistributedCode::replayLoop() {
	FileSystem::MarkReplayThread();
	for (;;) {
		Replay replay;
		{
			std::unique_lock<std::mutex> lock(replayMutex);
			replayReady.wait(lock, [] { return !replayQueue.empty(); });
			replay = std::move(replayQueue.front());
			replayQueue.pop_front();
		}
		if (replay.stop) {
			return;
		}
		switch (replay.request.type) {
		case CREATE_FILE: createFile(replay.request, replay.first, replay.second); break;
		case DELETE_FILE: deleteFile(replay.request, replay.first, replay.second); break;
		case CREATE_DIR: createDir(replay.request, replay.first, replay.second); break;
		case DELETE_DIR: deleteDir(replay.request, replay.first, replay.second); break;
		case RENAME: renameHandler(replay.request, replay.first, replay.second); break;
		case LINK: linkHandler(replay.request, replay.first, replay.second); break;
		default: break;
		}
	}
}

void DistributedCode::start() {
	vector<char> message;
	bool running = true;
	// Namespace replays are system calls on the local mount, served by this
	// peer's FUSE thread. That thread may itself be waiting for this listener
	// (an acknowledgement or a block slice of its own transfer), so replays
	// run on a separate thread and the listener never blocks on the kernel.
	std::thread replayer;
	if (!fsPath.empty()) {
		replayer = std::thread(&DistributedCode::replayLoop);
	}
	while (running) {
		// One self-describing message per request: probe for its size first.
		MPI_Status status;
		waitForRequest(status);
		int count = 0;
		MPI_Get_count(&status, MPI_BYTE, &count);
		const int source = status.MPI_SOURCE;
		message.resize(static_cast<size_t>(count));
		MPI_Recv(message.data(), count, MPI_BYTE, source, DAGONFS_TAG_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

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
		case DELETE_FILE:
		case CREATE_DIR:
		case DELETE_DIR:
		case RENAME:
		case LINK:
			if (replayer.joinable()) {
				enqueue(Replay{request, first, second, false});
			}
			break;
		case DELETE:
			Blocks::getInstance()->releaseLocalBlocks(request.io.inode);
			break;
		case TERMINATE:
			running = false;
			break;
		default:
			break;
		}
	}
	if (replayer.joinable()) {
		// Finish the replays already received, then end this peer's FUSE loop
		// (unless it has ended already: this peer originated the termination).
		enqueue(Replay{PeerRequest{}, string(), string(), true});
		replayer.join();
		if (!FileSystem::IsLocallyUnmounted()) {
			unmountFileSystem();
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

	if (mpiRank == sourceRank) {
		// Close-to-open consistency: the write completes only once every rank
		// has stored its slice and updated its view of the file.
		vector<MPI_Request> acks;
		for (int r = 0; r < mpiWorldSize; ++r) {
			if (r == mpiRank) continue;
			acks.emplace_back();
			MPI_Irecv(nullptr, 0, MPI_BYTE, r, DAGONFS_TAG_ACK, MPI_COMM_WORLD, &acks.back());
		}
		MPI_Waitall(static_cast<int>(acks.size()), acks.data(), MPI_STATUSES_IGNORE);
	} else {
		// Mirror the new size in the local replica of the file (or remember it
		// until the replayed create of the file has run).
		FileSystem::MirrorRemoteWrite(inode, fileSize);
		MPI_Send(nullptr, 0, MPI_BYTE, sourceRank, DAGONFS_TAG_ACK, MPI_COMM_WORLD);
	}
	if (mpiRank == sourceRank) {
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
