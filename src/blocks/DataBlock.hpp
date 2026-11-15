//
// Created by frank on 12/30/24.
//

#ifndef DATABLOCK_HPP
#define DATABLOCK_HPP

#include "../utils/fuse_headers.hpp"
#include "data_blocks_info.hpp"

/**
 * @brief One fixed-size block of file data held in the memory of the local rank.
 *
 * A DataBlock always owns its buffer: the memory is allocated by the
 * constructor and released by the destructor. Blocks are never shared between
 * MPI processes; every rank stores only the blocks assigned to it by the
 * contiguous block distribution described in DataBlockManager.
 */
class DataBlock {
private:
	fuse_ino_t inode;
	void *dataBlockAddress;
	unsigned int processRank;
	unsigned int progressiveNumber;

public:
	/**
	 * @brief Allocate an uninitialised block of FILE_SYSTEM_SINGLE_BLOCK_SIZE bytes.
	 *
	 * @param inode Owning inode number.
	 * @param rank Rank that stores the block (the local rank).
	 * @param progressiveNumber Global index of the block inside the file.
	 */
	DataBlock(fuse_ino_t inode, unsigned int rank, unsigned int progressiveNumber);
	~DataBlock();

	DataBlock(const DataBlock &) = delete;
	DataBlock &operator=(const DataBlock &) = delete;

	fuse_ino_t getInode() const { return inode; }
	void *getData() const { return dataBlockAddress; }
	unsigned int getRank() const { return processRank; }
	unsigned int getProgressiveNumber() const { return progressiveNumber; }
};

#endif //DATABLOCK_HPP
