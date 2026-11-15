//
// Created by frank on 12/30/24.
//

#include "MasterProcessCode.hpp"

#include <mpi.h>
#include "mpi_data.hpp"

#include "../blocks/Blocks.hpp"

using namespace std;

using namespace log4cplus;

MasterProcessCode *MasterProcessCode::instance = nullptr;

MasterProcessCode *MasterProcessCode::getInstance(int rank, int mpi_world_size) {
	if (instance == nullptr) {
		instance = new MasterProcessCode(rank, mpi_world_size);
	}

	return instance;
}

MasterProcessCode::MasterProcessCode(int rank, int mpi_world_size) {
	this->rank = rank;
	this->mpi_world_size = mpi_world_size;

	dataBlockManager = DataBlockManager::getInstance(mpi_world_size);
	MasterProcessLogger = Logger::getInstance("MasterProcess.logger - ");
	LogLevel ll = DAGONFS_LOG_LEVEL;
	MasterProcessLogger.setLogLevel(ll);

	lastWriteTime = 0.0;
	lastReadTime = 0.0;
	DAGonFSWriteSGElapsedTime = 0.0;
	DAGonFSReadSGElapsedTime = 0.0;
}

MasterProcessCode::~MasterProcessCode() = default;

void MasterProcessCode::broadcastHeader(RequestType type, fuse_ino_t inode, size_t fileSize) {
	RequestPacket request{};
	request.type = type;
	MPI_Bcast(&request, sizeof(RequestPacket), MPI_BYTE, 0, MPI_COMM_WORLD);
	IORequestPacket ioRequest{};
	ioRequest.inode = inode;
	ioRequest.fileSize = fileSize;
	ioRequest.reqSize = fileSize;
	ioRequest.offset = 0;
	MPI_Bcast(&ioRequest, sizeof(IORequestPacket), MPI_BYTE, 0, MPI_COMM_WORLD);
}

void MasterProcessCode::DAGonFS_Write(void* buffer, fuse_ino_t inode, size_t fileSize) {
	LOG4CPLUS_TRACE(MasterProcessLogger, MasterProcessLogger.getName() << "write of inode " << inode << " (" << fileSize << " bytes)");
	double start = MPI_Wtime();
	broadcastHeader(WRITE, inode, fileSize);
	DAGonFSWriteSGElapsedTime = dataBlockManager->scatterStore(buffer, inode, fileSize, rank, 0);
	lastWriteTime = MPI_Wtime() - start;
}

void *MasterProcessCode::DAGonFS_Read(fuse_ino_t inode, size_t fileSize, size_t reqSize, off_t offset) {
	(void) reqSize;
	(void) offset;
	LOG4CPLUS_TRACE(MasterProcessLogger, MasterProcessLogger.getName() << "read of inode " << inode << " (" << fileSize << " bytes)");
	double start = MPI_Wtime();
	broadcastHeader(READ, inode, fileSize);
	void *buffer = dataBlockManager->gatherLoad(inode, fileSize, rank, 0, &DAGonFSReadSGElapsedTime);
	lastReadTime = MPI_Wtime() - start;
	return buffer;
}

void MasterProcessCode::DAGonFS_Delete(fuse_ino_t inode) {
	LOG4CPLUS_TRACE(MasterProcessLogger, MasterProcessLogger.getName() << "delete of inode " << inode);
	broadcastHeader(DELETE, inode, 0);
	Blocks::getInstance()->releaseLocalBlocks(inode);
}

void MasterProcessCode::sendTermination() {
	RequestPacket request{};
	request.type = TERMINATE;
	MPI_Bcast(&request, sizeof(RequestPacket), MPI_BYTE, 0, MPI_COMM_WORLD);
}
