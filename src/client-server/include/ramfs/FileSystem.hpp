/** @file fuse_cpp_ramfs.hpp
 *  @copyright 2016 Peter Watkins. All rights reserved.
 */

//
// Created by frank on 11/29/24.
//
// This header and FileSystem.cpp are shared verbatim by the client-server and
// peer-to-peer trees. The peer-to-peer build defines DAGONFS_P2P.
//

#ifndef FILESYSTEM_HPP
#define FILESYSTEM_HPP

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <map>
#include <mutex>
#include <string>

#include "../utils/fuse_headers.hpp"
#include "../nodes/Nodes.hpp"
#include "../blocks/Blocks.hpp"
#ifdef DAGONFS_P2P
#include "../mpi/DistributedCode.hpp"
#else
#include "../mpi/MasterProcessCode.hpp"
#endif

#include "../utils/log_level.hpp"

/**
 * @brief The main FUSE code repository.
 *
 * The FUSE low-level file system that intercepts system calls on the mount
 * point. Metadata lives in the inode table of the front-end process; file
 * contents are distributed over the MPI ranks by the data plane.
 */
class FileSystem {
private:
    /** True if the filesystem is reclaiming inodes at the present point in time. */
    static bool m_reclaimingINodes;

    /** The constants defining the capabilities and sizes of the filesystem. */
    static struct statvfs m_stbuf;

    /** The number of blocks for the RAM file system */
    static const fsblkcnt_t kTotalBlocks = ~0;

    /** The total number of inodes */
    static const fsfilcnt_t kTotalInodes = ~0;

    /** The file system id */
    static const unsigned long kFilesystemId = 0xc13f944870434d8f;

    /** The maximum file name length (Linux NAME_MAX). */
    static const size_t kMaxFilenameLength = 255;

    /** The maximum file path length, currently 4096 characters including null */
    static const size_t kMaxPathLength = 4096;

    /** The threshold beyond which the file system starts to reclaim inodes labeled as deleted */
    static const size_t kINodeReclamationThreshold = 256;

    static Nodes *INodeManager;
    static Blocks *BlocksManager;
#ifdef DAGONFS_P2P
    static DistributedCode *distributedProcessCode;
#else
    static MasterProcessCode *MasterProcess;
#endif
    static log4cplus::Logger FSLogger;
    static int mpiRank;
    static int mpiWorldSize;

    static void show_usage(const char *progname);

    /** @brief Mount the file system and serve requests until it is unmounted. */
    static int mountAndServe(int argc, char *argv[]);

    /**
     * @brief Create a new i-node and insert it into the ram file system.
     *
     * @param type The type of the i-node (e.g. directory, file, symbolic link, etc.).
     * @param mode The permission given to the new i-node.
     * @param nlink The starting number of hard links for the new i-node.
     * @param gid The process group id for the new i-node.
     * @param uid The process user id for the new i-node.
     * @return The i-node number of the new i-node.
     */
    static fuse_ino_t RegisterINode(INodeType type, mode_t mode, nlink_t nlink, gid_t gid, uid_t uid);

    /** @brief Return the live inode @p ino, or nullptr if the number is invalid. */
    static INode *GetINode(fuse_ino_t ino);
    /** @brief Return @p ino as a directory, or nullptr. */
    static Directory *GetDirectory(fuse_ino_t ino);
    /** @brief Validate a directory-entry name; returns 0 or a POSIX error. */
    static int ValidateName(const char *name);
    /** @brief Apply a lookup-count decrement and reclaim the inode when unused. */
    static void ForgetINode(fuse_ino_t ino, uint64_t nlookup);
    /** @brief Set the size and the 512-byte block count of an inode. */
    static void SetSize(INode *inode, size_t size);
    /** @brief Refresh ctime (and mtime when @p modified) of an inode. */
    static void Touch(INode *inode, bool modified);

    /** @brief Make sure the file content is in the front-end buffer. */
    static bool LoadFileContent(File *file, fuse_ino_t ino);
    /** @brief Distribute the front-end buffer if it is dirty. */
    static void StoreFileContent(File *file, fuse_ino_t ino);
    /** @brief Release the distributed and local copies of a file without links. */
    static void DiscardFileContent(File *file, fuse_ino_t ino);
    /** @brief Key under which the data plane stores the content of a file. */
    static fuse_ino_t DataKey(File *file, fuse_ino_t ino);
#ifdef DAGONFS_P2P
    /** @brief Give a new file its cluster-wide identity and index it. */
    static uint64_t AssignGlobalId(File *file, fuse_ino_t ino, bool replay);
    static std::mutex globalIndexMutex;
    static std::map<uint64_t, fuse_ino_t> globalIndex;
#endif
    /** @brief Drop one name of @p inode; frees its data once unreferenced. */
    static void DropLink(INode *inode, fuse_ino_t ino);

#ifdef DAGONFS_P2P
    /** @brief Recompute the absolute path of a directory subtree after a rename. */
    static void UpdateDirectoryPaths(Directory *dir, Directory *parent, const std::string &path);
    /** @brief Absolute path (relative to the mount point) of one name of @p ino, or "". */
    static std::string FindPath(fuse_ino_t ino);
#endif

public:
    /** All the supported filesystem operations mapped to object-methods. */
    static struct fuse_lowlevel_ops FuseOperations;

    static FILE *timeFile;
    static double startWriteTime;
    static double endWriteTime;
    static double startReadTime;
    static double endReadTime;

#ifdef DAGONFS_P2P
    /*
     * Set by the MPI listener thread just before it replays an operation
     * received from another peer on the local mount point, so that the FUSE
     * handler does not broadcast it again.
     */
    static std::atomic<bool> createFileFromThread;
    static std::atomic<bool> deleteFileFromThread;
    static std::atomic<bool> createDirFromThread;
    static std::atomic<bool> deleteDirFromThread;
    static std::atomic<bool> renameFromThread;
    static std::atomic<bool> linkFromThread;
    /** Cluster-wide identity carried by a replayed create. */
    static std::atomic<uint64_t> pendingGlobalId;
    /** Local file with the given cluster-wide identity, or nullptr. */
    static File *FileForGlobalId(uint64_t id);
    static std::atomic<bool> unmountFromThread;
#endif

    FileSystem(int rank, int mpi_world_size);
    ~FileSystem();

    static int getMpiWorldSize() { return mpiWorldSize; }

    /** Session of the running FUSE loop (nullptr when not mounted). */
    static std::atomic<fuse_session *> activeSession;
    /** @brief Invalidate the kernel cache of @p ino (safe from any thread). */
    static void InvalidateINode(fuse_ino_t ino);

    /**
     * @brief Mount the file system and run the FUSE loop until unmounted.
     *
     * @param argc Arguments count.
     * @param argv Arguments list: argv[1] FUSE flag, argv[2] mount point.
     * @return Final status of our file system.
     */
    static int start(int argc, char *argv[]);

    //  FS operations (see fuse_lowlevel.h for the contract of each callback)
    static void FuseInit(void *userdata, struct fuse_conn_info *conn);
    static void FuseDestroy(void *userdata);
    static void FuseGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    static void FuseLookup(fuse_req_t req, fuse_ino_t parent, const char *name);
    static void FuseForget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup);
    static void FuseForgetMulti(fuse_req_t req, size_t count, struct fuse_forget_data *forgets);
    static void FuseSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi);
    static void FuseReadLink(fuse_req_t req, fuse_ino_t ino);
    static void FuseMknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev);
    static void FuseMkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode);
    static void FuseUnlink(fuse_req_t req, fuse_ino_t parent, const char *name);
    static void FuseRmdir(fuse_req_t req, fuse_ino_t parent, const char *name);
    static void FuseSymlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name);
    static void FuseRename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname, unsigned int flags);
    static void FuseLink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname);
    static void FuseOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    static void FuseFlush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    static void FuseRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    static void FuseFsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
    static void FuseOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    static void FuseReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
    static void FuseReleaseDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    static void FuseFsyncDir(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
    static void FuseStatfs(fuse_req_t req, fuse_ino_t ino);
#ifdef __APPLE__
    static void FuseSetXAttr(fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags, uint32_t position);
    static void FuseGetXAttr(fuse_req_t req, fuse_ino_t ino, const char *name, size_t size, uint32_t position);
#else
    static void FuseSetXAttr(fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags);
    static void FuseGetXAttr(fuse_req_t req, fuse_ino_t ino, const char *name, size_t size);
#endif
    static void FuseListXAttr(fuse_req_t req, fuse_ino_t ino, size_t size);
    static void FuseRemoveXAttr(fuse_req_t req, fuse_ino_t ino, const char *name);
    static void FuseAccess(fuse_req_t req, fuse_ino_t ino, int mask);
    static void FuseCreate(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi);
    static void FuseGetLock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock);
    static void FuseRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
    static void FuseWrite(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);

    static void UpdateUsedBlocks(ssize_t blocksAdded) { m_stbuf.f_bfree -= blocksAdded; m_stbuf.f_bavail -= blocksAdded;}
    static void UpdateUsedINodes(ssize_t inodesAdded) { m_stbuf.f_ffree -= inodesAdded; m_stbuf.f_favail -= inodesAdded;}
};

#endif //FILESYSTEM_HPP
