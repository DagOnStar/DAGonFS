//
// Created by frank on 12/30/24.
//

#ifndef MASTERPROCESSCODE_HPP
#define MASTERPROCESSCODE_HPP

#include "DataBlockManager.hpp"
#include "mpi_data.hpp"
#include "DistributedRead.hpp"
#include "DistributedWrite.hpp"

#include "../utils/log_level.hpp"

/**
 * @brief Data-plane client of the client-server model (rank 0).
 *
 * The master owns the namespace and the FUSE session. Each public operation
 * broadcasts a request header and then takes part, as root, in the matching
 * collective executed by NodeProcessCode on the storage ranks.
 */
class MasterProcessCode: public DistributedWrite, public DistributedRead {
private:
	static MasterProcessCode* instance;
	MasterProcessCode(int rank, int mpi_world_size);

	int rank;
	int mpi_world_size;

	DataBlockManager *dataBlockManager;
	log4cplus::Logger MasterProcessLogger;

	void broadcastHeader(RequestType type, fuse_ino_t inode, size_t fileSize);

public:
	double DAGonFSWriteSGElapsedTime;
	double DAGonFSReadSGElapsedTime;
	double lastWriteTime;
	double lastReadTime;
	static MasterProcessCode* getInstance(int rank, int mpi_world_size);

	~MasterProcessCode() override;
	/** Distribute @p fileSize bytes of @p buffer, replacing the stored copy. */
	void DAGonFS_Write(void* buffer, fuse_ino_t inode, size_t fileSize) override;
	/** Reassemble the @p fileSize bytes stored for @p inode (reqSize/offset are reserved). */
	void* DAGonFS_Read(fuse_ino_t inode, size_t fileSize, size_t reqSize, off_t offset) override;
	/** Release the blocks of @p inode on every rank. */
	void DAGonFS_Delete(fuse_ino_t inode);

	void sendTermination();
};

#endif //MASTERPROCESSCODE_HPP
