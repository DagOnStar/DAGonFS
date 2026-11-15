//
// Created by frank on 12/30/24.
//

#include "DataBlock.hpp"

#include <cstdio>
#include <cstdlib>

DataBlock::DataBlock(fuse_ino_t inode, unsigned int rank, unsigned int progressiveNumber)
	: inode(inode), dataBlockAddress(malloc(FILE_SYSTEM_SINGLE_BLOCK_SIZE)),
	  processRank(rank), progressiveNumber(progressiveNumber) {
	if (dataBlockAddress == nullptr) {
		// A storage rank cannot leave an MPI collective half-way through, so an
		// allocation failure here is unrecoverable.
		fprintf(stderr, "DAGonFS: rank %u cannot allocate a %d-byte data block\n",
		        rank, FILE_SYSTEM_SINGLE_BLOCK_SIZE);
		abort();
	}
}

DataBlock::~DataBlock() {
	free(dataBlockAddress);
}
