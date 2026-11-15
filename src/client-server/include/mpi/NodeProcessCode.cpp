//
// Created by frank on 12/30/24.
//

#include "NodeProcessCode.hpp"

#include <mpi.h>
#include "mpi_data.hpp"
#include "../blocks/Blocks.hpp"

using namespace std;

using namespace log4cplus;

NodeProcessCode *NodeProcessCode::instance = nullptr;

NodeProcessCode *NodeProcessCode::getInstance(int rank, int mpi_world_size) {
	if (instance == nullptr) {
		instance = new NodeProcessCode(rank, mpi_world_size);
	}

	return instance;
}

NodeProcessCode::NodeProcessCode(int rank, int mpi_world_size) {
	this->rank = rank;
	this->mpi_world_size = mpi_world_size;
	dataBlockManager = DataBlockManager::getInstance(mpi_world_size);
	LogLevel ll = DAGONFS_LOG_LEVEL;
	NodeProcessLogger = Logger::getInstance("NodeProcess.logger ");
	NodeProcessLogger.setLogLevel(ll);
}

NodeProcessCode::~NodeProcessCode() = default;

void NodeProcessCode::start() {
	bool running = true;

	while (running) {
		RequestPacket request{};
		IORequestPacket ioRequest{};
		MPI_Bcast(&request, sizeof(request), MPI_BYTE, 0, MPI_COMM_WORLD);
		switch (request.type) {
			case WRITE:
				MPI_Bcast(&ioRequest, sizeof(ioRequest), MPI_BYTE, 0, MPI_COMM_WORLD);
				DAGonFS_Write(nullptr, ioRequest.inode, ioRequest.fileSize);
				break;
			case READ:
				MPI_Bcast(&ioRequest, sizeof(ioRequest), MPI_BYTE, 0, MPI_COMM_WORLD);
				DAGonFS_Read(ioRequest.inode, ioRequest.fileSize, ioRequest.reqSize, ioRequest.offset);
				break;
			case DELETE:
				MPI_Bcast(&ioRequest, sizeof(ioRequest), MPI_BYTE, 0, MPI_COMM_WORLD);
				Blocks::getInstance()->releaseLocalBlocks(ioRequest.inode);
				break;
			case TERMINATE:
				running = false;
				break;
			default:
				LOG4CPLUS_WARN(NodeProcessLogger, NodeProcessLogger.getName() << "ignoring request type " << request.type);
				break;
		}
	}
}

void NodeProcessCode::DAGonFS_Write(void* buffer, fuse_ino_t inode, size_t fileSize) {
	(void) buffer;
	dataBlockManager->scatterStore(nullptr, inode, fileSize, rank, 0);
}

void* NodeProcessCode::DAGonFS_Read(fuse_ino_t inode, size_t fileSize, size_t reqSize, off_t offset) {
	(void) reqSize;
	(void) offset;
	dataBlockManager->gatherLoad(inode, fileSize, rank, 0, nullptr);
	return nullptr;
}
