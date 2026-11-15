//
// Created by frank on 12/30/24.
//

#include "DataBlockManager.hpp"

#include <climits>
#include <cstdio>
#include <cstdlib>

#include "../blocks/Blocks.hpp"

using namespace log4cplus;

DataBlockManager *DataBlockManager::instance = nullptr;

DataBlockManager *DataBlockManager::getInstance(int mpi_world_size) {
	if (instance == nullptr) {
		instance = new DataBlockManager(mpi_world_size);
	}

	return instance;
}

DataBlockManager::DataBlockManager(int mpi_world_size) : mpi_world_size(mpi_world_size), blockType(MPI_DATATYPE_NULL) {
	DataBlockManagerLogger = Logger::getInstance("DataBlockManager.logger - ");
	LogLevel ll = DAGONFS_LOG_LEVEL;
	DataBlockManagerLogger.setLogLevel(ll);
}

size_t DataBlockManager::blocksFor(size_t bytes) {
	return bytes / FILE_SYSTEM_SINGLE_BLOCK_SIZE + (bytes % FILE_SYSTEM_SINGLE_BLOCK_SIZE != 0);
}

void DataBlockManager::layout(size_t numberOfBlocks, vector<int> &counts, vector<int> &displs) const {
	if (numberOfBlocks > static_cast<size_t>(INT_MAX)) {
		fprintf(stderr, "DAGonFS: file of %zu blocks exceeds the MPI count range\n", numberOfBlocks);
		abort();
	}
	counts.assign(mpi_world_size, 0);
	displs.assign(mpi_world_size, 0);
	const size_t perRank = numberOfBlocks / mpi_world_size;
	const size_t remainder = numberOfBlocks % mpi_world_size;
	int offset = 0;
	for (int r = 0; r < mpi_world_size; ++r) {
		counts[r] = static_cast<int>(perRank + (static_cast<size_t>(r) < remainder));
		displs[r] = offset;
		offset += counts[r];
	}
}

MPI_Datatype DataBlockManager::localSliceType(const vector<void *> &addresses) const {
	// Describe the (non contiguous) local blocks as one datatype with absolute
	// displacements, so MPI reads/writes them in place with MPI_BOTTOM.
	const int count = static_cast<int>(addresses.size());
	vector<int> lengths(count, FILE_SYSTEM_SINGLE_BLOCK_SIZE);
	vector<MPI_Aint> displacements(count);
	for (int i = 0; i < count; ++i) {
		MPI_Get_address(addresses[i], &displacements[i]);
	}
	MPI_Datatype sliceType;
	MPI_Type_create_hindexed(count, lengths.data(), displacements.data(), MPI_BYTE, &sliceType);
	MPI_Type_commit(&sliceType);
	return sliceType;
}

double DataBlockManager::scatterStore(const void *buffer, fuse_ino_t inode, size_t fileSize, int rank, int root) {
	if (blockType == MPI_DATATYPE_NULL) {
		MPI_Type_contiguous(FILE_SYSTEM_SINGLE_BLOCK_SIZE, MPI_BYTE, &blockType);
		MPI_Type_commit(&blockType);
	}
	Blocks *blocks = Blocks::getInstance();
	const size_t numberOfBlocks = blocksFor(fileSize);
	if (numberOfBlocks == 0) {
		blocks->releaseLocalBlocks(inode);
		return 0.0;
	}

	vector<int> counts, displs;
	layout(numberOfBlocks, counts, displs);

	vector<DataBlock *> slice;
	vector<void *> addresses;
	slice.reserve(counts[rank]);
	addresses.reserve(counts[rank]);
	for (int i = 0; i < counts[rank]; ++i) {
		slice.push_back(new DataBlock(inode, rank, displs[rank] + i));
		addresses.push_back(slice.back()->getData());
	}

	double start = MPI_Wtime();
	if (counts[rank] > 0) {
		MPI_Datatype sliceType = localSliceType(addresses);
		MPI_Scatterv(buffer, counts.data(), displs.data(), blockType, MPI_BOTTOM, 1, sliceType, root, MPI_COMM_WORLD);
		MPI_Type_free(&sliceType);
	} else {
		MPI_Scatterv(buffer, counts.data(), displs.data(), blockType, nullptr, 0, blockType, root, MPI_COMM_WORLD);
	}
	double elapsed = MPI_Wtime() - start;

	blocks->replaceLocalBlocks(inode, std::move(slice));
	LOG4CPLUS_TRACE(DataBlockManagerLogger, DataBlockManagerLogger.getName() << "stored " << counts[rank]
	                << " local blocks of inode " << inode);
	return elapsed;
}

void *DataBlockManager::gatherLoad(fuse_ino_t inode, size_t fileSize, int rank, int root, double *elapsed) {
	if (blockType == MPI_DATATYPE_NULL) {
		MPI_Type_contiguous(FILE_SYSTEM_SINGLE_BLOCK_SIZE, MPI_BYTE, &blockType);
		MPI_Type_commit(&blockType);
	}
	if (elapsed != nullptr) *elapsed = 0.0;
	const size_t numberOfBlocks = blocksFor(fileSize);
	if (numberOfBlocks == 0) {
		return nullptr;
	}

	vector<int> counts, displs;
	layout(numberOfBlocks, counts, displs);

	void *gathered = nullptr;
	if (rank == root) {
		gathered = malloc(numberOfBlocks * FILE_SYSTEM_SINGLE_BLOCK_SIZE);
		if (gathered == nullptr) {
			fprintf(stderr, "DAGonFS: cannot allocate %zu bytes for a read\n", numberOfBlocks * FILE_SYSTEM_SINGLE_BLOCK_SIZE);
			abort();
		}
	}

	vector<void *> addresses = Blocks::getInstance()->localBlockAddresses(inode, counts[rank], rank, displs[rank]);

	double start = MPI_Wtime();
	if (counts[rank] > 0) {
		MPI_Datatype sliceType = localSliceType(addresses);
		MPI_Gatherv(MPI_BOTTOM, 1, sliceType, gathered, counts.data(), displs.data(), blockType, root, MPI_COMM_WORLD);
		MPI_Type_free(&sliceType);
	} else {
		MPI_Gatherv(nullptr, 0, blockType, gathered, counts.data(), displs.data(), blockType, root, MPI_COMM_WORLD);
	}
	if (elapsed != nullptr) *elapsed = MPI_Wtime() - start;
	return gathered;
}
