//
// Created by frank on 1/14/25.
//

#ifndef DISTRIBUTEDCODE_HPP
#define DISTRIBUTEDCODE_HPP

#include <string>

#include "DataBlockManager.hpp"
#include "mpi_data.hpp"
#include "../utils/fuse_headers.hpp"

/**
 * @brief MPI side of a peer.
 *
 * start() runs on a dedicated listener thread and serves the requests sent by
 * the other peers: it takes part in their data-plane collectives and replays
 * their namespace operations on the local mount point. The FUSE thread of the
 * same peer calls DAGonFS_Write / DAGonFS_Read as collective root.
 */
class DistributedCode {
private:
	static int mpiRank;
	static int mpiWorldSize;
	static std::string fsPath;
	static DistributedCode *instance;
	DistributedCode(int rank, int worldSize, const char *mountpointPath);

	static DataBlockManager *dataBlockManager;

	static double DAGonFSWriteSGElapsedTime;
	static double lastWriteTime;
	static double DAGonFSReadSGElapsedTime;
	static double lastReadTime;

	static std::string localPath(const std::string &relativePath);

public:
	static DistributedCode *getInstance(int rank, int worldSize, const char *mountpointPath);
	void setup();
	static void start();
	/** Collective rooted at @p sourceRank: replace the stored copy of @p inode. */
	static void DAGonFS_Write(int sourceRank, void *buffer, fuse_ino_t inode, size_t fileSize);
	/** Collective rooted at @p sourceRank: reassemble @p fileSize bytes of @p inode. */
	static void* DAGonFS_Read(int sourceRank, fuse_ino_t inode, size_t fileSize, size_t reqSize, off_t offset);
	static void createFile(const PeerRequest &request, const std::string &first, const std::string &second);
	static void deleteFile(const PeerRequest &request, const std::string &first, const std::string &second);
	static void createDir(const PeerRequest &request, const std::string &first, const std::string &second);
	static void deleteDir(const PeerRequest &request, const std::string &first, const std::string &second);
	static void renameHandler(const PeerRequest &request, const std::string &first, const std::string &second);
	static void linkHandler(const PeerRequest &request, const std::string &first, const std::string &second);
	static void unmountFileSystem();
	double getDAGonFSWriteSGElapsedTime() { return DAGonFSWriteSGElapsedTime; };
	double getDAGonFSReadSGElapsedTime() { return DAGonFSReadSGElapsedTime; };
	double getLastWriteTime() { return lastWriteTime; };
	double getLastReadTime() { return lastReadTime; };
};

#endif //DISTRIBUTEDCODE_HPP
