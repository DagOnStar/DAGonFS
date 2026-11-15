//
// Created by frank on 12/30/24.
//

#ifndef DATABLOCKMANAGER_HPP
#define DATABLOCKMANAGER_HPP

#include <cstddef>
#include <vector>
#include <mpi.h>

#include "../utils/fuse_headers.hpp"
#include "../utils/log_level.hpp"
#include "../blocks/DataBlock.hpp"

using namespace std;

/**
 * @brief Collective data plane shared by the client-server and peer-to-peer models.
 *
 * A file of S bytes is split into B = ceil(S / BLOCK) blocks. With P ranks,
 * rank r owns the contiguous range of floor(B / P) + (r < B mod P) blocks that
 * starts at the sum of the counts of the lower ranks. The layout depends only
 * on (B, P), so a later read can reconstruct it from the stored size alone and
 * no process-local address ever has to travel through MPI.
 */
class DataBlockManager {
private:
	static DataBlockManager* instance;
	explicit DataBlockManager(int mpi_world_size);

	int mpi_world_size;
	MPI_Datatype blockType;
	log4cplus::Logger DataBlockManagerLogger;

	MPI_Datatype localSliceType(const vector<void *> &addresses) const;

public:
	static DataBlockManager* getInstance(int mpi_world_size);

	/** Number of blocks needed to hold @p bytes. */
	static size_t blocksFor(size_t bytes);
	/** Block counts and displacements (in blocks) of every rank. */
	void layout(size_t numberOfBlocks, vector<int> &counts, vector<int> &displs) const;

	/**
	 * @brief Collective: distribute @p fileSize bytes of @p buffer (significant on root only).
	 *
	 * Every rank replaces its local slice of @p inode. The root buffer must hold
	 * blocksFor(fileSize) whole blocks. A size of zero releases all slices.
	 * @return Seconds spent in the MPI scatter.
	 */
	double scatterStore(const void *buffer, fuse_ino_t inode, size_t fileSize, int rank, int root);

	/**
	 * @brief Collective: reassemble the first @p fileSize bytes of @p inode on root.
	 *
	 * @param elapsed Receives the seconds spent in the MPI gather.
	 * @return On root, a malloc'd buffer of blocksFor(fileSize) blocks (nullptr if
	 *         the size is zero); nullptr on every other rank.
	 */
	void *gatherLoad(fuse_ino_t inode, size_t fileSize, int rank, int root, double *elapsed);
};

#endif //DATABLOCKMANAGER_HPP
