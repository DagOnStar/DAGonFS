//
// Created by frank on 12/30/24.
//

#ifndef NODEPROCESSCODE_HPP
#define NODEPROCESSCODE_HPP

#include "DataBlockManager.hpp"
#include "DistributedWrite.hpp"
#include "DistributedRead.hpp"
#include "../utils/log_level.hpp"

/**
 * @brief Storage rank of the client-server model (ranks 1..P-1).
 *
 * The worker loops on broadcast requests from the master and executes its
 * side of the data-plane collectives against its local block registry.
 */
class NodeProcessCode: public DistributedWrite, public DistributedRead {
private:
	static NodeProcessCode* instance;
	NodeProcessCode(int rank, int mpi_world_size);

	int rank;
	int mpi_world_size;

	DataBlockManager *dataBlockManager;
	log4cplus::Logger NodeProcessLogger;

public:
	static NodeProcessCode *getInstance(int rank, int mpi_world_size);

	~NodeProcessCode() override;
	void DAGonFS_Write(void* buffer, fuse_ino_t inode, size_t fileSize) override;
	void* DAGonFS_Read(fuse_ino_t inode, size_t fileSize, size_t reqSize, off_t offset) override;

	void start();
};

#endif //NODEPROCESSCODE_HPP
