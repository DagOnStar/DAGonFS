//
// Created by frank on 1/14/25.
//

#ifndef DISTRIBUTEDCODE_HPP
#define DISTRIBUTEDCODE_HPP

#include <string>

#include <mpi.h>

#include "DataBlockManager.hpp"
#include "mpi_data.hpp"
#include "../utils/fuse_headers.hpp"

/**
 * @brief MPI side of a rank, common to both deployment models.
 *
 * start() serves the requests sent by other ranks: it takes part in their
 * data-plane collectives, releases blocks, and (peer-to-peer) replays their
 * namespace operations on the local mount point.
 *
 * - Client-server: storage ranks run start() on their main thread; rank 0
 *   only originates requests (DAGonFS_Write / DAGonFS_Read as collective root).
 * - Peer-to-peer: every peer runs start() on a listener thread while its FUSE
 *   thread originates its own requests.
 */
class DistributedCode {
private:
	static int mpiRank;
	static int mpiWorldSize;
	/** Local mount point whose namespace is replayed; empty when this rank mounts nothing. */
	static std::string fsPath;
	static DistributedCode *instance;
	DistributedCode() = default;

	static DataBlockManager *dataBlockManager;

	static double DAGonFSWriteSGElapsedTime;
	static double lastWriteTime;
	static double DAGonFSReadSGElapsedTime;
	static double lastReadTime;

	static std::string localPath(const std::string &relativePath);
	/** Peer-to-peer: replay other peers' namespace operations, in arrival order. */
	static void replayLoop();
	/** Wait for the next request without holding the MPI progress lock. */
	static void waitForRequest(MPI_Status &status);

public:
	static DistributedCode *getInstance();
	/**
	 * @param replayMountPoint Peer-to-peer: the local mount point on which other
	 *        peers' namespace operations are replayed; empty otherwise.
	 */
	static void configure(int rank, int worldSize, const std::string &replayMountPoint);
	static void start();
	/** Transfer originated by @p sourceRank: replace the stored copy of @p inode. */
	static void DAGonFS_Write(int sourceRank, void *buffer, fuse_ino_t inode, size_t fileSize);
	/** Transfer originated by @p sourceRank: reassemble @p fileSize bytes of @p inode. */
	static void* DAGonFS_Read(int sourceRank, fuse_ino_t inode, size_t fileSize, size_t reqSize, off_t offset);
	static void createFile(const PeerRequest &request, const std::string &first, const std::string &second);
	static void deleteFile(const PeerRequest &request, const std::string &first, const std::string &second);
	static void createDir(const PeerRequest &request, const std::string &first, const std::string &second);
	static void deleteDir(const PeerRequest &request, const std::string &first, const std::string &second);
	static void renameHandler(const PeerRequest &request, const std::string &first, const std::string &second);
	static void linkHandler(const PeerRequest &request, const std::string &first, const std::string &second);
	static void symlinkHandler(const std::string &target, const std::string &newName);
	static void setAttrHandler(const PeerRequest &request, const std::string &name);
	static void setXAttrHandler(const PeerRequest &request, const std::string &name, const std::string &attribute,
	                            const std::string &value);
	static void removeXAttrHandler(const std::string &name, const std::string &attribute);
	static void mknodHandler(const PeerRequest &request, const std::string &name);
	static void unmountFileSystem();
	double getDAGonFSWriteSGElapsedTime() { return DAGonFSWriteSGElapsedTime; };
	double getDAGonFSReadSGElapsedTime() { return DAGonFSReadSGElapsedTime; };
	double getLastWriteTime() { return lastWriteTime; };
	double getLastReadTime() { return lastReadTime; };
};

#endif //DISTRIBUTEDCODE_HPP
