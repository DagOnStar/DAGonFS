//
// Created by frank on 12/30/24.
//

#ifndef BLOCKS_HPP
#define BLOCKS_HPP

#include <map>
#include <mutex>
#include <vector>
#include "../utils/fuse_headers.hpp"

#include "DataBlock.hpp"

using namespace std;

/**
 * @brief Registry of the data blocks stored by the local rank.
 *
 * For every inode the registry keeps the ordered slice of blocks that the
 * distribution assigned to this rank. All public methods are thread safe; the
 * peer-to-peer model updates the registry from both the FUSE thread and the
 * MPI listener thread.
 */
class Blocks {
private:
	static Blocks *instance;
	Blocks();

	map<fuse_ino_t, vector<DataBlock *> > FileSystemDataBlocks;
	mutable std::mutex registryMutex;

	static void deleteBlocks(vector<DataBlock *> &blocks);

public:
	static Blocks* getInstance();

	~Blocks();

	/**
	 * @brief Replace the local slice of an inode, releasing the previous blocks.
	 */
	void replaceLocalBlocks(fuse_ino_t inode, vector<DataBlock *> &&blocks);
	/**
	 * @brief Release every local block of an inode (file removed or truncated to zero).
	 */
	void releaseLocalBlocks(fuse_ino_t inode);
	/**
	 * @brief Return the data addresses of the local slice, in block order.
	 *
	 * Missing blocks (an inconsistent or absent slice) are materialised as
	 * zero-filled blocks so that the caller can always honour the collective.
	 */
	vector<void *> localBlockAddresses(fuse_ino_t inode, size_t expectedBlocks, unsigned int rank, unsigned int firstIndex);

	bool blockListExistForInode(fuse_ino_t inode) const;
	unsigned int getNumberOfUsedBlocksOfInode(fuse_ino_t inode) const;
	size_t getTotalBlockBytesOfInode(fuse_ino_t inode) const;
	bool hasNoBlocks(fuse_ino_t inode) const;
};

#endif //BLOCKS_HPP
