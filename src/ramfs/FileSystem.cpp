/** @file fuse_cpp_ramfs.cpp
 *  @copyright 2016 Peter Watkins. All rights reserved.
 */

//
// Created by frank on 11/29/24.
//
// One implementation serves both deployment models. Behaviour intrinsic to
// the model is selected at run time through FileSystem::peerToPeer.
//
// Every FUSE request handler replies exactly once: each fuse_reply_* call is
// either the last statement of the handler or immediately followed by return.
//

#include "FileSystem.hpp"
#include "../utils/ArgumentParser.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>
#include <limits>
#include <string>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <mpi.h>
#include <poll.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#ifdef __linux__
#include <linux/falloc.h>
#include <sys/syscall.h>
#endif

#include "../mpi/RequestSender.hpp"

#include "../utils/log_level.hpp"

#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE (1 << 0)
#endif
// fallocate(2) modes (Linux values; defined here for non-Linux builds).
#ifndef FALLOC_FL_KEEP_SIZE
#define FALLOC_FL_KEEP_SIZE 0x01
#endif
#ifndef FALLOC_FL_PUNCH_HOLE
#define FALLOC_FL_PUNCH_HOLE 0x02
#endif
#ifndef FALLOC_FL_ZERO_RANGE
#define FALLOC_FL_ZERO_RANGE 0x10
#endif

using namespace std;

using namespace log4cplus;

bool FileSystem::m_reclaimingINodes = false;

struct statvfs FileSystem::m_stbuf = {};

struct fuse_lowlevel_ops FileSystem::FuseOperations = {};

Nodes *FileSystem::INodeManager = nullptr;

Blocks *FileSystem::BlocksManager = nullptr;

DistributedCode *FileSystem::controlPlane = nullptr;
bool FileSystem::peerToPeer = false;
std::atomic<bool> FileSystem::createFileFromThread{false};
std::atomic<bool> FileSystem::deleteFileFromThread{false};
std::atomic<bool> FileSystem::createDirFromThread{false};
std::atomic<bool> FileSystem::deleteDirFromThread{false};
std::atomic<bool> FileSystem::renameFromThread{false};
std::atomic<bool> FileSystem::linkFromThread{false};
std::atomic<bool> FileSystem::symlinkFromThread{false};
std::atomic<bool> FileSystem::setAttrFromThread{false};
std::atomic<bool> FileSystem::setXAttrFromThread{false};
std::atomic<bool> FileSystem::removeXAttrFromThread{false};
std::atomic<bool> FileSystem::mknodFromThread{false};
dagonfs::LockTable FileSystem::lockTable;
std::mutex FileSystem::pendingLocksMutex;
std::map<uint64_t, FileSystem::PendingLock> FileSystem::pendingLocks;
std::atomic<uint64_t> FileSystem::nextLockToken{0};
std::set<std::tuple<uint64_t, uint64_t, bool>> FileSystem::lockedOwners;
std::atomic<uint64_t> FileSystem::pendingGlobalId{0};
std::mutex FileSystem::globalIndexMutex;
std::map<uint64_t, File *> FileSystem::globalIndex;
std::map<uint64_t, size_t> FileSystem::pendingSizes;
std::atomic<bool> FileSystem::loopEnded{false};
std::atomic<long> FileSystem::replayThreadId{-1};
std::atomic<bool> FileSystem::unmountFromThread{false};

Logger FileSystem::FSLogger = Logger::getInstance("FuseFileSystem.logger - ");

int FileSystem::mpiRank = 0;
int FileSystem::mpiWorldSize = 0;
std::atomic<fuse_session *> FileSystem::activeSession{nullptr};

FILE *FileSystem::timeFile = nullptr;
double FileSystem::startWriteTime = 0.0;
double FileSystem::endWriteTime = 0.0;
double FileSystem::startReadTime = 0.0;
double FileSystem::endReadTime = 0.0;

namespace {

bool isDirectory(INode *inode) {
    return dynamic_cast<Directory *>(inode) != nullptr;
}

long currentThreadId() {
#ifdef __linux__
    return static_cast<long>(syscall(SYS_gettid));
#else
    return -1;
#endif
}

void setTimeNow(struct stat &attr, bool access, bool modification, bool change) {
    timespec now{};
    clock_gettime(CLOCK_REALTIME, &now);
#ifdef __APPLE__
    if (access) attr.st_atimespec = now;
    if (modification) attr.st_mtimespec = now;
    if (change) attr.st_ctimespec = now;
#else
    if (access) attr.st_atim = now;
    if (modification) attr.st_mtim = now;
    if (change) attr.st_ctim = now;
#endif
}

} // namespace

/**
 * Constructor of our file system in RAM. It initializes all fuse operation to its methods.
 */
FileSystem::FileSystem(int rank, int mpi_world_size, dagonfs::Model model) {
    FuseOperations.init        = FileSystem::FuseInit;
    FuseOperations.destroy     = FileSystem::FuseDestroy;
    FuseOperations.getattr     = FileSystem::FuseGetAttr;
    FuseOperations.lookup      = FileSystem::FuseLookup;
    FuseOperations.forget      = FileSystem::FuseForget;
    FuseOperations.forget_multi = FileSystem::FuseForgetMulti;
    FuseOperations.setattr     = FileSystem::FuseSetAttr;
    FuseOperations.readlink    = FileSystem::FuseReadLink;
    FuseOperations.mknod       = FileSystem::FuseMknod;
    FuseOperations.mkdir       = FileSystem::FuseMkdir;
    FuseOperations.unlink      = FileSystem::FuseUnlink;
    FuseOperations.rmdir       = FileSystem::FuseRmdir;
    FuseOperations.symlink     = FileSystem::FuseSymlink;
    FuseOperations.rename      = FileSystem::FuseRename;
    FuseOperations.link        = FileSystem::FuseLink;
    FuseOperations.open        = FileSystem::FuseOpen;
    FuseOperations.read        = FileSystem::FuseRead;
    FuseOperations.write       = FileSystem::FuseWrite;
    FuseOperations.flush       = FileSystem::FuseFlush;
    FuseOperations.release     = FileSystem::FuseRelease;
    FuseOperations.fsync       = FileSystem::FuseFsync;
    FuseOperations.opendir     = FileSystem::FuseOpenDir;
    FuseOperations.readdir     = FileSystem::FuseReadDir;
    FuseOperations.releasedir  = FileSystem::FuseReleaseDir;
    FuseOperations.fsyncdir    = FileSystem::FuseFsyncDir;
    FuseOperations.statfs      = FileSystem::FuseStatfs;
    FuseOperations.setxattr    = FileSystem::FuseSetXAttr;
    FuseOperations.getxattr    = FileSystem::FuseGetXAttr;
    FuseOperations.listxattr   = FileSystem::FuseListXAttr;
    FuseOperations.removexattr = FileSystem::FuseRemoveXAttr;
    FuseOperations.access      = FileSystem::FuseAccess;
    FuseOperations.create      = FileSystem::FuseCreate;
    FuseOperations.getlk       = FileSystem::FuseGetLock;
    FuseOperations.setlk       = FileSystem::FuseSetLock;
    FuseOperations.flock       = FileSystem::FuseFlock;
    FuseOperations.fallocate   = FileSystem::FuseFallocate;
    FuseOperations.readdirplus = FileSystem::FuseReadDirPlus;
    FuseOperations.poll        = FileSystem::FusePoll;
    FuseOperations.ioctl       = FileSystem::FuseIoctl;
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 4)
    FuseOperations.copy_file_range = FileSystem::FuseCopyFileRange;
#endif
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 8)
    FuseOperations.lseek       = FileSystem::FuseLseek;
#endif

    m_stbuf.f_bsize   = Nodes::INodeBufBlockSize;      // File system block size
    m_stbuf.f_frsize  = Nodes::INodeBufBlockSize;      // Fundamental file system block size
    m_stbuf.f_blocks  = kTotalBlocks;          // Blocks on FS in units of f_frsize
    m_stbuf.f_bfree   = kTotalBlocks;          // Free blocks
    m_stbuf.f_bavail  = kTotalBlocks;          // Blocks available to non-root
    m_stbuf.f_files   = kTotalInodes;          // Total inodes
    m_stbuf.f_ffree   = kTotalInodes;          // Free inodes
    m_stbuf.f_favail  = kTotalInodes;          // Free inodes for non-root
    m_stbuf.f_fsid    = kFilesystemId;         // Filesystem ID
    m_stbuf.f_flag    = 0;                     // Mount flags (ST_RDONLY, ST_NOSUID, ...)
    m_stbuf.f_namemax = kMaxFilenameLength;    // Max file name length

    mpiRank = rank;
    mpiWorldSize = mpi_world_size;
    INodeManager = Nodes::getInstance();
    BlocksManager = Blocks::getInstance();
    peerToPeer = model == dagonfs::Model::PeerToPeer;
    // Coherence across peers: the kernel must ask this process for every name
    // and attribute, because the listener changes them asynchronously (a cache
    // invalidation can race with the reply that populates the cache).
    Nodes::cacheTimeout = peerToPeer ? 0.0 : 1.0;
    controlPlane = DistributedCode::getInstance();
    FSLogger = Logger::getInstance("FuseFileSystem.logger Process " + to_string(mpiRank) + " - ");

    LogLevel ll = DAGONFS_LOG_LEVEL;
    FSLogger.setLogLevel(ll);
}

FileSystem::~FileSystem() {
    if (timeFile != nullptr) {
        fclose(timeFile);
        timeFile = nullptr;
    }
}

namespace {

/** Create @p path and any missing parent (like mkdir -p); returns 0 or errno. */
int ensureDirectory(const string &path) {
    if (path.empty()) {
        return ENOENT;
    }
    for (size_t slash = path.find('/', 1); ; slash = path.find('/', slash + 1)) {
        const string prefix = path.substr(0, slash);
        if (!prefix.empty() && mkdir(prefix.c_str(), 0777) != 0 && errno != EEXIST) {
            return errno;
        }
        if (slash == string::npos) {
            break;
        }
    }
    struct stat st{};
    if (stat(path.c_str(), &st) != 0) {
        return errno;
    }
    return S_ISDIR(st.st_mode) ? 0 : ENOTDIR;
}

} // namespace

/**
 * Initialize the FUSE API connection and start the loop for the API calls. The parameters
 * are the same that are passed to main program by command line.
 */
int FileSystem::start(const std::string &mountPoint, bool debug) {
    const int ret = mountAndServe(mountPoint, debug);
    loopEnded = true;

    // Always release the other ranks, also when mounting failed: they are
    // blocked waiting for requests and would otherwise never terminate.
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "--> tell other MPI processes that the file system has been unmounted");
    if (peerToPeer) {
        // Also addressed to this peer's own listener thread. A peer that was
        // unmounted by a TERMINATE from another peer does not repeat it.
        if (!unmountFromThread.load()) {
            RequestSender::sendTerminationRequest(mpiRank, mpiWorldSize, true);
        }
    } else {
        // The front end has no listener thread: only the storage ranks.
        RequestSender::sendTerminationRequest(mpiRank, mpiWorldSize, false);
    }
    return ret;
}

int FileSystem::mountAndServe(const std::string &mountPoint, bool debug) {
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "--> Step 0: Creating mountpoint directory");
    const string fsPath = mountPoint;
    const int err = ensureDirectory(fsPath);
    if (err != 0) {
        cerr << "DAGonFS rank " << mpiRank << ": cannot create mountpoint " << fsPath << ": " << strerror(err) << endl;
        return err;
    }

    // Timing instrumentation is optional: failing to create it never prevents mounting.
    const string timesDirName = "/tmp/DAGonFS_times/";
    if (ensureDirectory(timesDirName) == 0) {
        const string timesFileName = timesDirName + "copy_times_rank" + to_string(mpiRank) + ".txt";
        timeFile = fopen(timesFileName.c_str(), "w");
    }
    if (timeFile == nullptr) {
        LOG4CPLUS_WARN(FSLogger, FSLogger.getName() << "timing file unavailable; continuing without it");
    }

    //LIBFUSE
    //Argument copying for fuse args: FUSE must never modify the caller's argv
    //(which MPI may also inspect).
    // FUSE always runs in the foreground: an MPI rank must stay attached to
    // its launcher. "-d" additionally traces every request.
    const char *fuseArgv[] = {"dagonfs", debug ? "-d" : "-f", fsPath.c_str(), nullptr};
    ArgumentParser parser;
    parser.copy_args(3, const_cast<char **>(fuseArgv));
    char **copied_argv_for_fuse = parser.getCopiedArgs();

    fuse_args args_for_fuse = FUSE_ARGS_INIT(3, copied_argv_for_fuse);
    fuse_cmdline_opts fuse_options{};

    //LIBFUSE
    //CLI arguments parsing to fill the options
    if (fuse_parse_cmdline(&args_for_fuse, &fuse_options) != 0) {
        show_usage("dagonfs");
        fuse_opt_free_args(&args_for_fuse);
        return EINVAL;
    }

    int ret = 0;
    if (fuse_options.show_help) {
        cout << "***** Command Line Help fuse fuse_cmdline_help() *****" << endl;
        fuse_cmdline_help();
        cout << "***** Low Level Help fuse_lowlevel_help() *****" << endl;
        fuse_lowlevel_help();
    } else if (fuse_options.show_version) {
        cout << "FUSE library version: " << fuse_pkgversion() << endl;
    } else if (fuse_options.mountpoint == nullptr) {
        LOG4CPLUS_ERROR(FSLogger, FSLogger.getName() << "no mountpoint given");
        ret = 2;
    } else {
        LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "--> Step 1: mountpoint " << fuse_options.mountpoint);

        fuse_session *session = fuse_session_new(&args_for_fuse, &(FuseOperations), sizeof(FuseOperations), nullptr);
        if (session == nullptr) {
            cerr << "DAGonFS rank " << mpiRank << ": fuse_session_new() failed" << endl;
            ret = -1;
        } else {
            if (fuse_set_signal_handlers(session) == 0) {
                LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "--> Step 3: fuse_signal_handlers() OK");
                if (fuse_session_mount(session, fuse_options.mountpoint) == 0) {
                    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "--> Step 4: fuse_session_mount() Success");
                    // MPI processes must stay in the foreground: daemonizing
                    // would fork away from the MPI runtime.
                    fuse_daemonize(1);
                    activeSession = session;
                    ret = fuse_session_loop(session);
                    activeSession = nullptr;
                    loopEnded = true;
                    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "***** Loop terminated *****");
                    fuse_session_unmount(session);
                    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "--> Step 5: fuse_session_unmount() Success");
                } else {
                    cerr << "DAGonFS rank " << mpiRank << ": cannot mount " << fuse_options.mountpoint << endl;
                    ret = -1;
                }
                fuse_remove_signal_handlers(session);
            } else {
                ret = -1;
            }
            fuse_session_destroy(session);
        }
        // Only an empty mountpoint is removed; a non-empty directory is left untouched.
        rmdir(fsPath.c_str());
    }
    free(fuse_options.mountpoint);
    fuse_opt_free_args(&args_for_fuse);

    return ret;
}

void FileSystem::show_usage(const char *progname){
    cerr << dagonfs::usage(progname);
}

/**
 * Initializes the filesystem. Creates the root directory. The UID and GID are those of the creating process.
 */
void FileSystem::FuseInit(void *userdata, struct fuse_conn_info *conn) {
    (void) userdata;
    // Byte-range and flock locks are served by the volume's lock table, so
    // that they are visible to every process of every peer.
    if (conn->capable & FUSE_CAP_POSIX_LOCKS) {
        conn->want |= FUSE_CAP_POSIX_LOCKS;
    }
    if (conn->capable & FUSE_CAP_FLOCK_LOCKS) {
        conn->want |= FUSE_CAP_FLOCK_LOCKS;
    }
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() <<  "Starting FuseInit()...");

    FileSystem::m_reclaimingINodes = false;

    m_stbuf.f_bfree  = m_stbuf.f_blocks;
    m_stbuf.f_bavail = m_stbuf.f_blocks;
    m_stbuf.f_ffree  = m_stbuf.f_files;
    m_stbuf.f_favail = m_stbuf.f_files;

    const gid_t gid = getgid();
    const uid_t uid = getuid();

    // Inode 0 is a placeholder so that the root directory gets FUSE_ROOT_ID (1).
    RegisterINode(SPECIAL_INODE_TYPE_NO_BLOCK, 0777, 0, gid, uid);

    // The root directory has links from ".", from ".." and from the mount point.
    fuse_ino_t rootIno = RegisterINode(DIRECTORY, S_IFDIR | 0777, 3, gid, uid);
    Directory *rootDir = dynamic_cast<Directory *>(INodeManager->getINodeByINodeNumber(rootIno));
    rootDir->setDirName("/");

    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "FuseInit() completed!");
}

void FileSystem::FuseDestroy(void *userdata) {
    (void) userdata;
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "FUSE session destroyed");
}

/**
 * This method create a new i-node and add it to the list of i-nodes of the file system.
 * The i-node number is generated in this method, and it's retured to the caller.
 */
fuse_ino_t FileSystem::RegisterINode(INodeType type, mode_t mode, nlink_t nlink, gid_t gid, uid_t uid) {
    //Stop reclaiming inodes if there are no more to reclaim.
    if (INodeManager->getNumberOfDeletedINodes() == 0) {
        m_reclaimingINodes = false;
    }

    INode *inode_p = INodeManager->createEmptyINode(type);

    fuse_ino_t ino;
    if (m_reclaimingINodes) {
        ino = INodeManager->reclaimINode();
        INode *del_p = INodeManager->getINodeByINodeNumber(ino);
        INodeManager->setINodeAt(ino, inode_p);
        delete del_p;
    } else {
        ino = INodeManager->AddINode(inode_p);
        FileSystem::UpdateUsedINodes(1);
    }

    INodeManager->InitializeINode(inode_p, ino, mode, nlink, gid, uid);

    return ino;
}

INode *FileSystem::GetINode(fuse_ino_t ino) {
    if (ino == 0 || ino >= INodeManager->getNumberOfINodes()) {
        return nullptr;
    }
    return INodeManager->getINodeByINodeNumber(ino);
}

Directory *FileSystem::GetDirectory(fuse_ino_t ino) {
    return dynamic_cast<Directory *>(GetINode(ino));
}

int FileSystem::ValidateName(const char *name) {
    if (name == nullptr || name[0] == '\0') {
        return ENOENT;
    }
    if (strlen(name) > kMaxFilenameLength) {
        return ENAMETOOLONG;
    }
    if (strchr(name, '/') != nullptr) {
        return EINVAL;
    }
    return 0;
}

void FileSystem::SetSize(INode *inode, size_t size) {
    inode->m_fuseEntryParam.attr.st_size = static_cast<off_t>(size);
    // st_blocks is expressed in 512-byte units by POSIX.
    inode->m_fuseEntryParam.attr.st_blocks = static_cast<blkcnt_t>((size + 511) / 512);
}

void FileSystem::Touch(INode *inode, bool modified) {
    setTimeNow(inode->m_fuseEntryParam.attr, false, modified, true);
}

bool FileSystem::LoadFileContent(File *file, fuse_ino_t ino) {
    // Peer-to-peer: another peer wrote the file back since the buffer was
    // loaded. A clean buffer is reloaded, so that a reader that synchronises
    // with the writer (a lock, or the writer's close) observes the new data;
    // unflushed local writes are kept (the last write-back wins).
    if (file->m_remoteChanged.exchange(false) && file->m_buf != nullptr && !file->isWaitingForWriting()) {
        file->releaseBuffer();
    }
    const size_t size = static_cast<size_t>(file->m_fuseEntryParam.attr.st_size);
    if (file->m_buf != nullptr) {
        // Already open here. In the peer-to-peer model another peer may have
        // grown the file meanwhile: keep the buffer at least as large as the
        // size we report (the open descriptor keeps its snapshot of the data).
        return file->ensureCapacity(size);
    }
    if (file->m_storedSize > 0) {
        startReadTime = MPI_Wtime();
        void *buffer = controlPlane->DAGonFS_Read(mpiRank, DataKey(file, ino), file->m_storedSize, file->m_storedSize, 0);
        const size_t capacity = DataBlockManager::blocksFor(file->m_storedSize) * FILE_SYSTEM_SINGLE_BLOCK_SIZE;
        // Bytes past the stored size are padding of the last block.
        memset(static_cast<char *>(buffer) + file->m_storedSize, 0, capacity - file->m_storedSize);
        file->adoptBuffer(buffer, capacity);
        endReadTime = MPI_Wtime();
        if (timeFile != nullptr) {
            fprintf(timeFile, "read inode=%lu bytes=%zu total=%f collective=%f\n",
                    static_cast<unsigned long>(ino), file->m_storedSize, endReadTime - startReadTime,
                    controlPlane->getDAGonFSReadSGElapsedTime()
            );
            fflush(timeFile);
        }
    }
    // The logical size may exceed the stored copy (ftruncate grew the file).
    return file->ensureCapacity(size);
}

void FileSystem::StoreFileContent(File *file, fuse_ino_t ino) {
    if (!file->isWaitingForWriting()) {
        return;
    }
    if (file->HasNoLinks()) {
        // Nobody can reach this content any more; do not resurrect its blocks.
        file->removeWaiting();
        return;
    }
    const size_t size = static_cast<size_t>(file->m_fuseEntryParam.attr.st_size);
    if (size > 0 && !file->ensureCapacity(size)) {
        LOG4CPLUS_ERROR(FSLogger, FSLogger.getName() << "cannot allocate the send buffer of inode " << ino);
        return;
    }
    if (size > 0) {
        // Zero the padding of the last block so that stored data is deterministic.
        const size_t padded = DataBlockManager::blocksFor(size) * FILE_SYSTEM_SINGLE_BLOCK_SIZE;
        memset(static_cast<char *>(file->m_buf) + size, 0, padded - size);
    }
    const double start = MPI_Wtime();
    controlPlane->DAGonFS_Write(mpiRank, file->m_buf, DataKey(file, ino), size);
    endWriteTime = MPI_Wtime();
    file->m_storedSize = size;
    file->removeWaiting();
    if (timeFile != nullptr) {
        fprintf(timeFile, "write inode=%lu bytes=%zu total=%f since-open=%f collective=%f\n",
                static_cast<unsigned long>(ino), size, endWriteTime - start, endWriteTime - startWriteTime,
                controlPlane->getDAGonFSWriteSGElapsedTime()
        );
        fflush(timeFile);
    }
}

void FileSystem::DiscardFileContent(File *file, fuse_ino_t ino) {
    file->releaseBuffer();
    file->removeWaiting();
    {
        std::lock_guard<std::mutex> lock(globalIndexMutex);
        globalIndex.erase(file->m_globalId);
        pendingSizes.erase(file->m_globalId);
    }
    const fuse_ino_t key = DataKey(file, ino);
    if (file->m_storedSize == 0 && BlocksManager->hasNoBlocks(key)) {
        return;
    }
    BlocksManager->releaseLocalBlocks(key);
    if (!peerToPeer) {
        // Storage ranks release their slices on request. (Every peer replays
        // the unlink itself, so peers release their own slices.)
        RequestSender::sendDeleteRequest(key, mpiRank, mpiWorldSize);
    }
    file->m_storedSize = 0;
}

void FileSystem::DropLink(INode *inode, fuse_ino_t ino) {
    inode->RemoveHardLink();
    Touch(inode, false);
    File *file = dynamic_cast<File *>(inode);
    if (file != nullptr && file->HasNoLinks() && file->m_openCount == 0) {
        DiscardFileContent(file, ino);
    }
}

fuse_ino_t FileSystem::DataKey(File *file, fuse_ino_t ino) {
    // Peers number inodes independently: the data plane addresses a file by
    // its cluster-wide identity, never by a local inode number.
    (void) ino;
    return file->m_globalId;
}

void FileSystem::InvalidateINode(fuse_ino_t ino) {
    fuse_session *session = activeSession.load();
    if (session != nullptr) {
        // Drop the kernel's cached attributes and pages of an inode whose
        // content was changed by another rank.
        fuse_lowlevel_notify_inval_inode(session, ino, 0, 0);
    }
}

uint64_t FileSystem::AssignGlobalId(File *file, fuse_ino_t ino, bool replay) {
    uint64_t id = replay ? pendingGlobalId.exchange(0) : 0;
    if (id == 0) {
        // (creator rank + 1) in the top 16 bits, creator inode in the rest.
        id = (static_cast<uint64_t>(mpiRank + 1) << 48) | (static_cast<uint64_t>(ino) & ((1ULL << 48) - 1));
    }
    file->m_globalId = id;
    std::lock_guard<std::mutex> lock(globalIndexMutex);
    globalIndex[id] = file;
    // Data written by the creator may have arrived before this replayed create.
    if (const auto pending = pendingSizes.find(id); pending != pendingSizes.end()) {
        ApplyStoredSize(file, pending->second);
        pendingSizes.erase(pending);
    }
    return id;
}

void FileSystem::MarkReplayThread() {
    replayThreadId = currentThreadId();
}

bool FileSystem::IsReplay(fuse_req_t req, std::atomic<bool> &flag) {
    // The flag is always consumed so that it can never leak into a later request.
    const bool flagged = flag.exchange(false);
    if (!peerToPeer) {
        return false;
    }
    const long replayer = replayThreadId.load();
    if (replayer > 0) {
        // Linux reports the requesting thread: exact, even when a local request
        // was queued in the kernel just before the replayed one.
        return fuse_req_ctx(req)->pid == static_cast<pid_t>(replayer);
    }
    return flagged;
}

void FileSystem::ApplyStoredSize(File *file, size_t size) {
    file->m_fuseEntryParam.attr.st_size = static_cast<off_t>(size);
    file->m_fuseEntryParam.attr.st_blocks = static_cast<blkcnt_t>((size + 511) / 512);
    file->m_storedSize = size;
}

void FileSystem::MirrorRemoteWrite(uint64_t id, size_t size) {
    fuse_ino_t ino = 0;
    {
        std::lock_guard<std::mutex> lock(globalIndexMutex);
        const auto entry = globalIndex.find(id);
        if (entry == globalIndex.end()) {
            pendingSizes[id] = size;
            return;
        }
        ApplyStoredSize(entry->second, size);
        entry->second->m_remoteChanged = true;
        ino = entry->second->inodeNumber;
    }
    InvalidateINode(ino);
}

File *FileSystem::FileForGlobalId(uint64_t id) {
    std::lock_guard<std::mutex> lock(globalIndexMutex);
    const auto entry = globalIndex.find(id);
    return entry == globalIndex.end() ? nullptr : entry->second;
}

void FileSystem::FuseGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi){
    (void) fi;
    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    fuse_reply_attr(req, &(inode->m_fuseEntryParam.attr), Nodes::cacheTimeout);
}

void FileSystem::FuseLookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() <<  "Lookup -> FuseRamFs::FuseLookup()");

    INode *parentInode = GetINode(parent);
    if (parentInode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *dir = dynamic_cast<Directory *>(parentInode);
    if (dir == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    if (name != nullptr && strlen(name) > kMaxFilenameLength) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }

    fuse_ino_t ino = dir->ChildINodeNumberWithName(string(name));
    INode *inode = ino == static_cast<fuse_ino_t>(-1) ? nullptr : GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    INodeManager->LookupINode(ino);
    fuse_reply_entry(req, &(inode->m_fuseEntryParam));
}

/**
 * Decrease the lookup count of an inode. An inode that is no longer known to
 * the kernel and has no hard link is queued for reclamation.
 */
void FileSystem::ForgetINode(fuse_ino_t ino, uint64_t nlookup) {
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr) {
        return;
    }
    inode_p->Forget(nlookup);
    if (inode_p->Forgotten() && inode_p->HasNoLinks()) {
        File *file = dynamic_cast<File *>(inode_p);
        if (file != nullptr && file->m_openCount > 0) {
            return;
        }
        if (file != nullptr) {
            DiscardFileContent(file, ino);
        }
        inode_p->markForDeletion();
        if (!peerToPeer) {
            // Inode numbers are reused only in the client-server model. Peers
            // keep deleted inodes so that a replayed operation can never
            // resolve to a recycled number.
            INodeManager->DeleteINode(ino);
            if (INodeManager->getNumberOfDeletedINodes() > FileSystem::kINodeReclamationThreshold) {
                m_reclaimingINodes = true;
            }
        }
    }
}

void FileSystem::FuseForget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup) {
    ForgetINode(ino, nlookup);
    fuse_reply_none(req);
}

void FileSystem::FuseForgetMulti(fuse_req_t req, size_t count, struct fuse_forget_data *forgets) {
    for (size_t index = 0; index < count; ++index) {
        ForgetINode(forgets[index].ino, forgets[index].nlookup);
    }
    fuse_reply_none(req);
}

void FileSystem::FuseSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat* attr, int to_set, struct fuse_file_info* fi) {
    (void) fi;
    const bool replay = IsReplay(req, setAttrFromThread);
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "Setting -> FuseRamFs::FuseSetAttr()");

    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    struct stat &current = inode->m_fuseEntryParam.attr;

    if (to_set & FUSE_SET_ATTR_SIZE) {
        File *file = dynamic_cast<File *>(inode);
        if (file == nullptr) {
            fuse_reply_err(req, isDirectory(inode) ? EISDIR : EINVAL);
            return;
        }
        if (attr->st_size < 0) {
            fuse_reply_err(req, EINVAL);
            return;
        }
        const size_t newSize = static_cast<size_t>(attr->st_size);
        const size_t oldSize = static_cast<size_t>(current.st_size);
        if (newSize != oldSize || file->m_storedSize != newSize) {
            const bool wasLoaded = file->m_buf != nullptr;
            // Truncation to zero never needs the previous content.
            if (newSize == 0) {
                file->releaseBuffer();
            } else if (!LoadFileContent(file, ino) || !file->ensureCapacity(newSize)) {
                fuse_reply_err(req, ENOMEM);
                return;
            }
            if (newSize < oldSize && file->m_buf != nullptr) {
                // Bytes beyond the new end must read back as zeros if the file grows again.
                memset(static_cast<char *>(file->m_buf) + newSize, 0, file->m_capacity - newSize);
            }
            SetSize(file, newSize);
            file->setWaiting();
            if (file->m_openCount == 0) {
                // truncate(2) on a closed file: persist immediately.
                StoreFileContent(file, ino);
                file->releaseBuffer();
            } else if (!wasLoaded && newSize == 0) {
                file->releaseBuffer();
            }
            Touch(file, true);
        }
    }
    if (to_set & FUSE_SET_ATTR_MODE) {
        // Only permission bits can change; the file type is immutable.
        current.st_mode = (current.st_mode & S_IFMT) | (attr->st_mode & 07777);
    }
    if (to_set & FUSE_SET_ATTR_UID) {
        current.st_uid = attr->st_uid;
    }
    if (to_set & FUSE_SET_ATTR_GID) {
        current.st_gid = attr->st_gid;
    }
#ifdef __APPLE__
    if (to_set & FUSE_SET_ATTR_ATIME) current.st_atimespec = attr->st_atimespec;
    if (to_set & FUSE_SET_ATTR_MTIME) current.st_mtimespec = attr->st_mtimespec;
    if (to_set & FUSE_SET_ATTR_CTIME) current.st_ctimespec = attr->st_ctimespec;
    if (to_set & FUSE_SET_ATTR_BTIME) current.st_birthtimespec = attr->st_birthtimespec;
    if (to_set & FUSE_SET_ATTR_FLAGS) current.st_flags = attr->st_flags;
#else
    if (to_set & FUSE_SET_ATTR_ATIME) current.st_atim = attr->st_atim;
    if (to_set & FUSE_SET_ATTR_MTIME) current.st_mtim = attr->st_mtim;
    if (to_set & FUSE_SET_ATTR_CTIME) current.st_ctim = attr->st_ctim;
#endif
    // utimensat(UTIME_NOW): the kernel sets the *_NOW flag and no timestamp.
    setTimeNow(current, (to_set & FUSE_SET_ATTR_ATIME_NOW) != 0, (to_set & FUSE_SET_ATTR_MTIME_NOW) != 0, false);
#ifdef __APPLE__
    if (!(to_set & FUSE_SET_ATTR_CTIME))
#else
    if (!(to_set & FUSE_SET_ATTR_CTIME))
#endif
    {
        setTimeNow(current, false, false, true);
    }

    // Peer-to-peer: replicate metadata changes. The size travels with the data
    // plane; times are sent as applied here, so every peer stores the same value.
    MetadataPacket meta{};
    if (peerToPeer && !replay) {
        if (to_set & FUSE_SET_ATTR_MODE) meta.mask |= DAGONFS_ATTR_MODE;
        if (to_set & FUSE_SET_ATTR_UID) meta.mask |= DAGONFS_ATTR_UID;
        if (to_set & FUSE_SET_ATTR_GID) meta.mask |= DAGONFS_ATTR_GID;
        if (to_set & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_ATIME_NOW)) meta.mask |= DAGONFS_ATTR_ATIME;
        if (to_set & (FUSE_SET_ATTR_MTIME | FUSE_SET_ATTR_MTIME_NOW)) meta.mask |= DAGONFS_ATTR_MTIME;
        meta.mode = static_cast<uint32_t>(current.st_mode & 07777);
        meta.uid = static_cast<uint32_t>(current.st_uid);
        meta.gid = static_cast<uint32_t>(current.st_gid);
#ifdef __APPLE__
        meta.atimeSeconds = current.st_atimespec.tv_sec;
        meta.atimeNanoseconds = current.st_atimespec.tv_nsec;
        meta.mtimeSeconds = current.st_mtimespec.tv_sec;
        meta.mtimeNanoseconds = current.st_mtimespec.tv_nsec;
#else
        meta.atimeSeconds = current.st_atim.tv_sec;
        meta.atimeNanoseconds = current.st_atim.tv_nsec;
        meta.mtimeSeconds = current.st_mtim.tv_sec;
        meta.mtimeNanoseconds = current.st_mtim.tv_nsec;
#endif
    }
    fuse_reply_attr(req, &current, Nodes::cacheTimeout);
    if (peerToPeer && meta.mask != 0) {
        const string path = ReplicatedPath(ino);
        if (!path.empty() && path.size() < kMaxPathLength) {
            RequestSender::sendSetAttrRequest(path, meta, mpiRank, mpiWorldSize);
        }
    }
}

void FileSystem::FuseReadLink(fuse_req_t req, fuse_ino_t ino) {
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    SymbolicLink *link_p = dynamic_cast<SymbolicLink *>(inode_p);
    if (link_p == nullptr) {
        fuse_reply_err(req, EINVAL);
        return;
    }

    fuse_reply_readlink(req, link_p->Link().c_str());
}

void FileSystem::FuseMknod(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode, dev_t rdev) {
    const bool replay = IsReplay(req, mknodFromThread);
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "Making node -> FuseRamFs::FuseMknod()");

    INode *parentInode = GetINode(parent);
    if (parentInode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *parentDir_p = dynamic_cast<Directory *>(parentInode);
    if (parentDir_p == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    const int nameError = ValidateName(name);
    if (nameError != 0) {
        fuse_reply_err(req, nameError);
        return;
    }
    if (parentDir_p->ChildINodeNumberWithName(string(name)) != static_cast<fuse_ino_t>(-1)) {
        fuse_reply_err(req, EEXIST);
        return;
    }
    const string fileName = parentDir_p->getDirName() + name;
    if (peerToPeer && !replay && fileName.size() >= kMaxPathLength) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    const struct fuse_ctx* ctx_p = fuse_req_ctx(req);
    if (S_ISFIFO(mode) || S_ISSOCK(mode) || S_ISCHR(mode) || S_ISBLK(mode)) {
        // FIFOs, sockets and device nodes hold no data here: once the kernel
        // knows their type it serves open, read and write itself.
        fuse_ino_t ino = RegisterINode(SPECIAL_FILE, (mode & S_IFMT) | (mode & 07777), 1, ctx_p->gid, ctx_p->uid);
        INode *inode_p = INodeManager->getINodeByINodeNumber(ino);
        inode_p->m_fuseEntryParam.attr.st_rdev = rdev;
        parentDir_p->UpdateChild(string(name), ino);
        Touch(parentDir_p, true);
        inode_p->Lookup();
        fuse_reply_entry(req, &(inode_p->m_fuseEntryParam));
        if (peerToPeer && !replay) {
            RequestSender::sendMknodRequest(fileName, static_cast<uint32_t>(mode & S_IFMT),
                                            static_cast<uint32_t>(mode & 07777), static_cast<uint64_t>(rdev),
                                            mpiRank, mpiWorldSize);
        }
        return;
    }
    if (!S_ISREG(mode)) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    // Peers replay a regular-file mknod as an exclusive create of the same path.
    fuse_ino_t ino = RegisterINode(REGULAR_FILE, S_IFREG | (mode & 07777), 1, ctx_p->gid, ctx_p->uid);
    INode *inode_p = INodeManager->getINodeByINodeNumber(ino);

    parentDir_p->UpdateChild(string(name), ino);
    Touch(parentDir_p, true);
    const uint64_t globalId = AssignGlobalId(dynamic_cast<File *>(inode_p), ino, false);

    inode_p->Lookup();
    fuse_reply_entry(req, &(inode_p->m_fuseEntryParam));
    if (peerToPeer && !replay) {
        RequestSender::sendCreateFileRequest(fileName, globalId, mpiRank, mpiWorldSize);
    }
}

void FileSystem::FuseMkdir(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode) {
    // Consume the replay flag first so that an early error cannot leak it
    // into the next, locally originated, operation.
    const bool replay = IsReplay(req, createDirFromThread);
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "Making directory '" << name << "' -> FuseRamFs::FuseMkdir()");

    INode *parentInode = GetINode(parent);
    if (parentInode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *parentDir_p = dynamic_cast<Directory *>(parentInode);
    if (parentDir_p == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    const int nameError = ValidateName(name);
    if (nameError != 0) {
        fuse_reply_err(req, nameError);
        return;
    }
    if (parentDir_p->ChildINodeNumberWithName(string(name)) != static_cast<fuse_ino_t>(-1)) {
        fuse_reply_err(req, EEXIST);
        return;
    }
    const string dirName = parentDir_p->getDirName() + name + "/";
    if (peerToPeer && !replay && dirName.size() >= kMaxPathLength) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }

    const struct fuse_ctx* ctx_p = fuse_req_ctx(req);
    fuse_ino_t ino = RegisterINode(DIRECTORY, S_IFDIR | (mode & 07777), 2, ctx_p->gid, ctx_p->uid);
    Directory *dir_p = dynamic_cast<Directory *>(INodeManager->getINodeByINodeNumber(ino));
    dir_p->setParent(parentDir_p);
    dir_p->setDirName(dirName.c_str());

    parentDir_p->UpdateChild(string(name), ino);
    parentDir_p->AddHardLink();
    Touch(parentDir_p, true);

    dir_p->Lookup();
    fuse_reply_entry(req, &(dir_p->m_fuseEntryParam));

    if (peerToPeer && !replay) {
        RequestSender::sendCreateDirectoryRequest(dirName, mpiRank, mpiWorldSize);
    }
}

/**
 * Remove a name. The inode data are released once no hard link and no open
 * descriptor refers to it.
 */
void FileSystem::FuseUnlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
    // Consume the replay flag first so that an early error cannot leak it
    // into the next, locally originated, operation.
    const bool replay = IsReplay(req, deleteFileFromThread);
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "\tunlink " << name << " in " << parent);

    INode *parentInode = GetINode(parent);
    if (parentInode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *parentDir_p = dynamic_cast<Directory *>(parentInode);
    if (parentDir_p == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }

    fuse_ino_t ino = parentDir_p->ChildINodeNumberWithName(string(name));
    INode *inode_p = ino == static_cast<fuse_ino_t>(-1) ? nullptr : GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    if (isDirectory(inode_p)) {
        fuse_reply_err(req, EISDIR);
        return;
    }
    const string fileName = parentDir_p->getDirName() + name;

    parentDir_p->DeleteChild(string(name));
    Touch(parentDir_p, true);
    DropLink(inode_p, ino);

    fuse_reply_err(req, 0);

    if (peerToPeer && !replay) {
        RequestSender::sendDeleteFileRequest(fileName, mpiRank, mpiWorldSize);
    }
}

/**
 * Remove an empty directory.
 */
void FileSystem::FuseRmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
    // Consume the replay flag first so that an early error cannot leak it
    // into the next, locally originated, operation.
    const bool replay = IsReplay(req, deleteDirFromThread);
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "Removing directory -> FuseRamFs::FuseRmdir");

    INode *parentInode = GetINode(parent);
    if (parentInode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *parentDir_p = dynamic_cast<Directory *>(parentInode);
    if (parentDir_p == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }

    fuse_ino_t ino = parentDir_p->ChildINodeNumberWithName(string(name));
    INode *inode_p = ino == static_cast<fuse_ino_t>(-1) ? nullptr : GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *dir_p = dynamic_cast<Directory *>(inode_p);
    if (dir_p == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    if (dir_p->hasChildren()) {
        fuse_reply_err(req, ENOTEMPTY);
        return;
    }
    const string dirName = dir_p->getDirName();

    // The removed directory's ".." no longer links the parent.
    parentDir_p->RemoveHardLink();
    while (!dir_p->HasNoLinks()) {
        dir_p->RemoveHardLink();
    }
    parentDir_p->DeleteChild(string(name));
    Touch(parentDir_p, true);

    fuse_reply_err(req, 0);

    if (peerToPeer && !replay) {
        RequestSender::sendDeleteDirectoryRequest(dirName, mpiRank, mpiWorldSize);
    }
}

void FileSystem::FuseSymlink(fuse_req_t req, const char* link, fuse_ino_t parent, const char* name) {
    const bool replay = IsReplay(req, symlinkFromThread);
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() <<  "Creating symbolic link -> FuseRamFs::FuseSymlink");
    INode *parent_p = GetINode(parent);
    if (parent_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *dir = dynamic_cast<Directory *>(parent_p);
    if (dir == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    const int nameError = ValidateName(name);
    if (nameError != 0) {
        fuse_reply_err(req, nameError);
        return;
    }
    if (link == nullptr || strlen(link) >= kMaxPathLength) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    if (dir->ChildINodeNumberWithName(string(name)) != static_cast<fuse_ino_t>(-1)) {
        fuse_reply_err(req, EEXIST);
        return;
    }
    const string linkName = dir->getDirName() + name;
    if (peerToPeer && !replay && linkName.size() >= kMaxPathLength) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }

    const struct fuse_ctx* ctx_p = fuse_req_ctx(req);

    fuse_ino_t ino = RegisterINode(SYMBOLIC_LINK, S_IFLNK | 0777, 1, ctx_p->gid, ctx_p->uid);
    SymbolicLink *symLink = dynamic_cast<SymbolicLink *>(INodeManager->getINodeByINodeNumber(ino));
    symLink->setLink(link);
    SetSize(symLink, strlen(link));

    dir->UpdateChild(string(name), ino);
    Touch(dir, true);

    symLink->Lookup();
    fuse_reply_entry(req, &(symLink->m_fuseEntryParam));
    if (peerToPeer && !replay) {
        RequestSender::sendSymlinkRequest(string(link), linkName, mpiRank, mpiWorldSize);
    }
}

string FileSystem::ReplicatedPath(fuse_ino_t ino) {
    return ino == FUSE_ROOT_ID ? string("/") : FindPath(ino);
}

string FileSystem::FindPath(fuse_ino_t target) {
    // Breadth-first search from the root for any name of the target inode.
    vector<Directory *> pending;
    Directory *root = GetDirectory(FUSE_ROOT_ID);
    if (root != nullptr) pending.push_back(root);
    for (size_t index = 0; index < pending.size(); ++index) {
        Directory *dir = pending[index];
        for (const auto &[childName, childIno] : dir->Children()) {
            if (childIno == target) {
                return dir->getDirName() + childName;
            }
            Directory *child = GetDirectory(childIno);
            if (child != nullptr) pending.push_back(child);
        }
    }
    return string();
}

void FileSystem::UpdateDirectoryPaths(Directory *dir, Directory *parent, const string &path) {
    dir->setParent(parent);
    dir->setDirName(path.c_str());
    for (const auto &[childName, childIno] : dir->Children()) {
        Directory *child = GetDirectory(childIno);
        if (child != nullptr) {
            UpdateDirectoryPaths(child, dir, path + childName + "/");
        }
    }
}

void FileSystem::FuseRename(fuse_req_t req, fuse_ino_t parent, const char* name, fuse_ino_t newparent, const char* newname, unsigned int flags) {
    // Consume the replay flag first so that an early error cannot leak it
    // into the next, locally originated, operation.
    const bool replay = IsReplay(req, renameFromThread);
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "Renaming -> FuseRamFs::FuseRename()");

    // Only RENAME_NOREPLACE is supported; RENAME_EXCHANGE and whiteouts are not.
    if (flags & ~static_cast<unsigned int>(RENAME_NOREPLACE)) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    Directory *parentDir = GetDirectory(parent);
    Directory *newParentDir = GetDirectory(newparent);
    if (parentDir == nullptr || newParentDir == nullptr) {
        fuse_reply_err(req, GetINode(parent) == nullptr || GetINode(newparent) == nullptr ? ENOENT : ENOTDIR);
        return;
    }
    const int nameError = ValidateName(newname);
    if (nameError != 0) {
        fuse_reply_err(req, nameError);
        return;
    }

    fuse_ino_t ino = parentDir->ChildINodeNumberWithName(string(name));
    INode *inode_p = ino == static_cast<fuse_ino_t>(-1) ? nullptr : GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    const bool sourceIsDir = isDirectory(inode_p);

    fuse_ino_t existingIno = newParentDir->ChildINodeNumberWithName(string(newname));
    INode *existing = existingIno == static_cast<fuse_ino_t>(-1) ? nullptr : GetINode(existingIno);
    if (existing != nullptr && (flags & RENAME_NOREPLACE)) {
        fuse_reply_err(req, EEXIST);
        return;
    }
    if (existingIno == ino) {
        // Both names refer to the same inode: POSIX requires a successful no-op.
        fuse_reply_err(req, 0);
        return;
    }
    if (sourceIsDir) {
        // A directory cannot be moved below itself.
        for (Directory *ancestor = newParentDir; ancestor != nullptr; ancestor = ancestor->getParent()) {
            if (ancestor == inode_p) {
                fuse_reply_err(req, EINVAL);
                return;
            }
        }
    }
    if (existing != nullptr) {
        const bool targetIsDir = isDirectory(existing);
        if (sourceIsDir && !targetIsDir) {
            fuse_reply_err(req, ENOTDIR);
            return;
        }
        if (!sourceIsDir && targetIsDir) {
            fuse_reply_err(req, EISDIR);
            return;
        }
        if (targetIsDir && dynamic_cast<Directory *>(existing)->hasChildren()) {
            fuse_reply_err(req, ENOTEMPTY);
            return;
        }
    }

    const string oldName = parentDir->getDirName() + name;
    const string newName = newParentDir->getDirName() + newname;
    if (peerToPeer && !replay && newName.size() + 1 >= kMaxPathLength) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }

    if (existing != nullptr) {
        newParentDir->DeleteChild(string(newname));
        if (isDirectory(existing)) {
            newParentDir->RemoveHardLink();
            while (!existing->HasNoLinks()) {
                existing->RemoveHardLink();
            }
        } else {
            DropLink(existing, existingIno);
        }
    }
    if (sourceIsDir && parentDir != newParentDir) {
        // The moved directory's ".." now links the new parent.
        parentDir->RemoveHardLink();
        newParentDir->AddHardLink();
    }

    parentDir->DeleteChild(string(name));
    newParentDir->UpdateChild(string(newname), ino);
    Touch(parentDir, true);
    Touch(newParentDir, true);
    Touch(inode_p, false);

    if (sourceIsDir) {
        UpdateDirectoryPaths(dynamic_cast<Directory *>(inode_p), newParentDir, newName + "/");
    }

    fuse_reply_err(req, 0);

    if (peerToPeer && !replay) {
        RequestSender::sendRenameRequest(oldName, newName, mpiRank, mpiWorldSize);
    }
}

void FileSystem::FuseLink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char* newname) {
    // Consume the replay flag first so that an early error cannot leak it
    // into the next, locally originated, operation.
    const bool replay = IsReplay(req, linkFromThread);
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() <<  "Creating hard link -> FuseRamFs::FuseLink");

    INode *parentInode = GetINode(newparent);
    if (parentInode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *newParentDir_p = dynamic_cast<Directory *>(parentInode);
    if (newParentDir_p == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    if (isDirectory(inode_p)) {
        fuse_reply_err(req, EPERM);
        return;
    }
    const int nameError = ValidateName(newname);
    if (nameError != 0) {
        fuse_reply_err(req, nameError);
        return;
    }
    if (newParentDir_p->ChildINodeNumberWithName(string(newname)) != static_cast<fuse_ino_t>(-1)) {
        fuse_reply_err(req, EEXIST);
        return;
    }
    string targetName;
    const string newName = newParentDir_p->getDirName() + newname;
    if (peerToPeer && !replay) {
        // Peers address files by path: resolve one existing name of the target.
        targetName = FindPath(ino);
        if (targetName.empty()) {
            fuse_reply_err(req, ENOENT);
            return;
        }
        if (targetName.size() >= kMaxPathLength || newName.size() >= kMaxPathLength) {
            fuse_reply_err(req, ENAMETOOLONG);
            return;
        }
    }
    newParentDir_p->UpdateChild(string(newname), ino);
    inode_p->AddHardLink();
    Touch(inode_p, false);
    Touch(newParentDir_p, true);

    inode_p->Lookup();
    fuse_reply_entry(req, &(inode_p->m_fuseEntryParam));
    if (peerToPeer && !replay) {
        RequestSender::sendLinkRequest(targetName, newName, mpiRank, mpiWorldSize);
    }
}

void FileSystem::FuseOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "Opening file -> FuseRamFs::FuseOpen");

    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    if (isDirectory(inode)) {
        fuse_reply_err(req, EISDIR);
        return;
    }
    File *file_p = dynamic_cast<File *>(inode);
    if (file_p == nullptr) {
        fuse_reply_err(req, EINVAL);
        return;
    }

    if (fi->flags & O_TRUNC) {
        // The previous content is discarded: no distributed read is needed.
        if (file_p->m_fuseEntryParam.attr.st_size > 0 || file_p->m_storedSize > 0) {
            file_p->setWaiting();
        }
        SetSize(file_p, 0);
        if (file_p->m_buf != nullptr) {
            memset(file_p->m_buf, 0, file_p->m_capacity);
        }
        Touch(file_p, true);
    } else if (!LoadFileContent(file_p, ino)) {
        fuse_reply_err(req, ENOMEM);
        return;
    }
    if ((fi->flags & O_ACCMODE) != O_RDONLY) {
        startWriteTime = MPI_Wtime();
    }
    file_p->m_openCount++;

    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() << "\topen for " << ino << " with flags " << fi->flags);
    fuse_reply_open(req, fi);
}

/**
 * Called on every close(2) of a descriptor: publish dirty content to the ranks.
 */
void FileSystem::FuseFlush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    File *file_p = dynamic_cast<File *>(inode);
    if (file_p != nullptr) {
        // Write back first: the next holder of a lock must see the data.
        StoreFileContent(file_p, ino);
        // POSIX: closing any descriptor of a file releases all the record
        // locks that the process holds on it. The reply waits for the table.
        const uint64_t key = LockKey(file_p);
        if (IsLockCoordinator() || lockedOwners.erase({key, fi->lock_owner, false}) > 0) {
            LockPacket release{};
            release.owner = fi->lock_owner;
            SubmitLock(req, LOCK_RELEASE, key, release);
            return;
        }
    }
    fuse_reply_err(req, 0);
}

/**
 * Invoked when the last reference to an open file handle goes away.
 */
void FileSystem::FuseRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    (void) fi;
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    File *file_p = dynamic_cast<File *>(inode_p);
    if (file_p == nullptr) {
        fuse_reply_err(req, isDirectory(inode_p) ? EISDIR : EINVAL);
        return;
    }

    const uint64_t key = LockKey(file_p);
    if (file_p->m_openCount > 0) {
        file_p->m_openCount--;
    }
    if (file_p->m_openCount == 0) {
        if (file_p->HasNoLinks()) {
            DiscardFileContent(file_p, ino);
        } else {
            StoreFileContent(file_p, ino);
            file_p->releaseBuffer();
        }
    }
    // The last close of an open file description releases its flock lock.
    if (fi->flock_release && (IsLockCoordinator() || lockedOwners.erase({key, fi->lock_owner, true}) > 0)) {
        LockPacket release{};
        release.owner = fi->lock_owner;
        release.flags = DAGONFS_LOCK_FLOCK;
        SubmitLock(req, LOCK_RELEASE, key, release);
        return;
    }
    fuse_reply_err(req, 0);
}

void FileSystem::FuseFsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info* fi) {
    (void) datasync;
    (void) fi;
    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    File *file_p = dynamic_cast<File *>(inode);
    if (file_p != nullptr) {
        StoreFileContent(file_p, ino);
    }
    fuse_reply_err(req, 0);
}

void FileSystem::FuseOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    if (!isDirectory(inode)) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    fuse_reply_open(req, fi);
}

namespace {

/**
 * Fill a readdir (or readdirplus) reply. Directory offsets are positions in
 * the ordered child map: 1 and 2 follow "." and "..", child k is followed by
 * offset k + 3. Offsets therefore stay valid across calls, survive concurrent
 * insertions/removals (which POSIX allows to be reported or not) and never
 * refer to heap memory. @p add returns false when the entry does not fit.
 */
template <typename Add>
void listDirectory(Directory *dir, fuse_ino_t ino, off_t off, Add add) {
    off_t position = off;
    if (position == 0) {
        if (!add(".", ino, position + 1)) return;
        ++position;
    }
    if (position == 1) {
        Directory *parentDir = dir->getParent();
        const fuse_ino_t parentIno = parentDir == nullptr ? ino : parentDir->inodeNumber;
        if (!add("..", parentIno, position + 1)) return;
        ++position;
    }
    auto &children = dir->Children();
    const size_t skip = static_cast<size_t>(position - 2);
    if (skip >= children.size()) {
        return;
    }
    auto child = children.begin();
    std::advance(child, skip);
    for (; child != children.end(); ++child) {
        if (!add(child->first.c_str(), child->second, position + 1)) return;
        ++position;
    }
}

} // namespace

void FileSystem::FuseReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info* fi) {
    (void) fi;

    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *dir = dynamic_cast<Directory *>(inode);
    if (dir == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    if (off < 0) {
        fuse_reply_err(req, EINVAL);
        return;
    }

    vector<char> buf(size);
    size_t bytesAdded = 0;
    listDirectory(dir, ino, off, [&](const char *entryName, fuse_ino_t entryIno, off_t next) {
        struct stat stbuf{};
        INode *entry = GetINode(entryIno);
        stbuf.st_ino = entryIno;
        stbuf.st_mode = entry == nullptr ? 0 : (entry->m_fuseEntryParam.attr.st_mode & S_IFMT);
        const size_t needed = fuse_add_direntry(req, buf.data() + bytesAdded, size - bytesAdded, entryName, &stbuf, next);
        if (needed > size - bytesAdded) {
            return false;
        }
        bytesAdded += needed;
        return true;
    });
    fuse_reply_buf(req, bytesAdded == 0 ? nullptr : buf.data(), bytesAdded);
}

/**
 * readdir that also returns each entry's attributes (used by ls -l, find,
 * os.scandir): saves one lookup per entry. Every returned entry other than
 * "." and ".." counts as a lookup, exactly as if FuseLookup had replied.
 */
void FileSystem::FuseReadDirPlus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info* fi) {
    (void) fi;

    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *dir = dynamic_cast<Directory *>(inode);
    if (dir == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    if (off < 0) {
        fuse_reply_err(req, EINVAL);
        return;
    }

    vector<char> buf(size);
    size_t bytesAdded = 0;
    listDirectory(dir, ino, off, [&](const char *entryName, fuse_ino_t entryIno, off_t next) {
        INode *entry = GetINode(entryIno);
        if (entry == nullptr) {
            return true; // vanished meanwhile: skip it
        }
        const bool dotEntry = strcmp(entryName, ".") == 0 || strcmp(entryName, "..") == 0;
        const size_t needed = fuse_add_direntry_plus(req, buf.data() + bytesAdded, size - bytesAdded, entryName,
                                                     &entry->m_fuseEntryParam, next);
        if (needed > size - bytesAdded) {
            return false;
        }
        bytesAdded += needed;
        if (!dotEntry) {
            INodeManager->LookupINode(entryIno);
        }
        return true;
    });
    fuse_reply_buf(req, bytesAdded == 0 ? nullptr : buf.data(), bytesAdded);
}

void FileSystem::FuseReleaseDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
    (void) fi;
    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    if (!isDirectory(inode)) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    fuse_reply_err(req, 0);
}

/**
 * Since the file system is in RAM, it's not necessary synchronize directory content on HDD/SSD.
 */
void FileSystem::FuseFsyncDir(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info* fi) {
    (void) datasync;
    (void) fi;
    if (GetINode(ino) == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    fuse_reply_err(req, 0);
}

void FileSystem::FuseStatfs(fuse_req_t req, fuse_ino_t ino) {
    (void) ino;
    fuse_reply_statfs(req, &m_stbuf);
}

#if defined(__APPLE__) && FUSE_DARWIN_ENABLE_EXTENSIONS
void FileSystem::FuseSetXAttr(fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags, uint32_t position)
#else
void FileSystem::FuseSetXAttr(fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags)
#endif
{
    const bool replay = IsReplay(req, setXAttrFromThread);
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
#if !defined(__APPLE__) || !FUSE_DARWIN_ENABLE_EXTENSIONS
    const uint32_t position = 0;
#endif
    if (name == nullptr || strncmp(name, "system.posix_acl_", 17) == 0) {
        // POSIX ACLs are not enforced, so they are not stored either: setfacl
        // must not report success for an ACL that would be ignored.
        fuse_reply_err(req, name == nullptr ? EINVAL : EOPNOTSUPP);
        return;
    }
    string path;
    if (peerToPeer && !replay) {
        if (size > DAGONFS_MAX_REQUEST_VALUE || position != 0) {
            fuse_reply_err(req, size > DAGONFS_MAX_REQUEST_VALUE ? E2BIG : ENOTSUP);
            return;
        }
        path = ReplicatedPath(ino);
    }
    const int ret_val = inode_p->SetXAttr(string(name), value, size, flags, position);
    if (ret_val != 0) {
        fuse_reply_err(req, ret_val);
        return;
    }
    Touch(inode_p, false);
    fuse_reply_err(req, 0);
    if (peerToPeer && !path.empty() && path.size() < kMaxPathLength) {
        RequestSender::sendSetXAttrRequest(path, string(name), string(value, size), flags, mpiRank, mpiWorldSize);
    }
}

#if defined(__APPLE__) && FUSE_DARWIN_ENABLE_EXTENSIONS
void FileSystem::FuseGetXAttr(fuse_req_t req, fuse_ino_t ino, const char *name, size_t size, uint32_t position)
#else
void FileSystem::FuseGetXAttr(fuse_req_t req, fuse_ino_t ino, const char *name, size_t size)
#endif
{
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
#if !defined(__APPLE__) || !FUSE_DARWIN_ENABLE_EXTENSIONS
    const uint32_t position = 0;
#endif

    auto &xattrs = inode_p->GetXAttr();
    const auto attribute = xattrs.find(name);
    if (attribute == xattrs.end()) {
#ifdef __APPLE__
        fuse_reply_err(req, ENOATTR);
#else
        fuse_reply_err(req, ENODATA);
#endif
        return;
    }
    const size_t valueSize = attribute->second.second;
    if (position > valueSize) {
        fuse_reply_err(req, ERANGE);
        return;
    }
    const size_t available = valueSize - position;

    // A zero size asks for the length of the value.
    if (size == 0) {
        fuse_reply_xattr(req, available);
        return;
    }
    // The caller's buffer must be able to hold the whole value.
    if (size < available) {
        fuse_reply_err(req, ERANGE);
        return;
    }
    fuse_reply_buf(req, static_cast<const char *>(attribute->second.first) + position, available);
}

void FileSystem::FuseListXAttr(fuse_req_t req, fuse_ino_t ino, size_t size) {
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    auto &xattrs = inode_p->GetXAttr();
    size_t listSize = 0;
    for (const auto &attribute : xattrs) {
        listSize += attribute.first.size() + 1;
    }

    // A zero size asks for the length of the list.
    if (size == 0) {
        fuse_reply_xattr(req, listSize);
        return;
    }
    if (size < listSize) {
        fuse_reply_err(req, ERANGE);
        return;
    }

    string names;
    names.reserve(listSize);
    for (const auto &attribute : xattrs) {
        names.append(attribute.first);
        names.push_back('\0');
    }
    fuse_reply_buf(req, names.data(), names.size());
}

void FileSystem::FuseRemoveXAttr(fuse_req_t req, fuse_ino_t ino, const char* name) {
    const bool replay = IsReplay(req, removeXAttrFromThread);
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    const int ret_val = inode_p->RemoveXAttr(string(name));
    if (ret_val != 0) {
        fuse_reply_err(req, ret_val);
        return;
    }
    Touch(inode_p, false);
    fuse_reply_err(req, 0);
    if (peerToPeer && !replay) {
        const string path = ReplicatedPath(ino);
        if (!path.empty() && path.size() < kMaxPathLength) {
            RequestSender::sendRemoveXAttrRequest(path, string(name), mpiRank, mpiWorldSize);
        }
    }
}

void FileSystem::FuseAccess(fuse_req_t req, fuse_ino_t ino, int mask) {
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr || inode_p->isDeleted()) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    const struct fuse_ctx* ctx_p = fuse_req_ctx(req);
    const struct stat &attr = inode_p->m_fuseEntryParam.attr;

    if (mask == F_OK || ctx_p->uid == 0) {
        fuse_reply_err(req, 0);
        return;
    }

    // Select the owner, group or other permission triplet, as the kernel does.
    mode_t granted;
    if (ctx_p->uid == attr.st_uid) {
        granted = (attr.st_mode >> 6) & 07;
    } else if (ctx_p->gid == attr.st_gid) {
        granted = (attr.st_mode >> 3) & 07;
    } else {
        granted = attr.st_mode & 07;
    }
    fuse_reply_err(req, (granted & mask) == static_cast<mode_t>(mask) ? 0 : EACCES);
}

void FileSystem::FuseCreate(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode, struct fuse_file_info* fi) {
    // Consume the replay flag first so that an early error cannot leak it
    // into the next, locally originated, operation.
    const bool replay = IsReplay(req, createFileFromThread);
    LOG4CPLUS_TRACE(FSLogger, FSLogger.getName() <<  "Creating " << name << " -> FuseRamFs::FuseCreate");

    INode *parent_p = GetINode(parent);
    if (parent_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    Directory *parentDir_p = dynamic_cast<Directory *>(parent_p);
    if (parentDir_p == nullptr) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    const int nameError = ValidateName(name);
    if (nameError != 0) {
        fuse_reply_err(req, nameError);
        return;
    }
    if (parentDir_p->ChildINodeNumberWithName(string(name)) != static_cast<fuse_ino_t>(-1)) {
        fuse_reply_err(req, EEXIST);
        return;
    }
    const string fileName = parentDir_p->getDirName() + name;
    if (peerToPeer && !replay && fileName.size() >= kMaxPathLength) {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }

    const struct fuse_ctx* ctx_p = fuse_req_ctx(req);
    fuse_ino_t ino = RegisterINode(REGULAR_FILE, S_IFREG | (mode & 07777), 1, ctx_p->gid, ctx_p->uid);
    File *file_p = dynamic_cast<File *>(INodeManager->getINodeByINodeNumber(ino));

    parentDir_p->UpdateChild(string(name), ino);
    Touch(parentDir_p, true);
    const uint64_t globalId = AssignGlobalId(file_p, ino, replay);

    if ((fi->flags & O_ACCMODE) != O_RDONLY) {
        startWriteTime = MPI_Wtime();
    }
    file_p->m_openCount++;
    file_p->Lookup();
    fuse_reply_create(req, &(file_p->m_fuseEntryParam), fi);

    if (peerToPeer && !replay) {
        RequestSender::sendCreateFileRequest(fileName, globalId, mpiRank, mpiWorldSize);
    }
}

// ------------------------------------------------------------------ locks

bool FileSystem::IsLockCoordinator() {
    return !peerToPeer || mpiRank == DAGONFS_LOCK_COORDINATOR;
}

uint64_t FileSystem::LockKey(File *file) {
    // The cluster-wide identity: every peer names the file the same way.
    return file->m_globalId;
}

namespace {

/** struct flock (l_start, l_len) -> inclusive byte range; false if invalid. */
bool lockRange(const struct flock *lock, int64_t &start, int64_t &end) {
    int64_t first = lock->l_start;
    int64_t length = lock->l_len;
    if (length < 0) {
        first += length;
        length = -length;
    }
    if (first < 0) {
        return false;
    }
    start = first;
    end = length == 0 || length - 1 > dagonfs::kLockToEnd - first ? dagonfs::kLockToEnd : first + length - 1;
    return true;
}

} // namespace

std::vector<dagonfs::LockReply> FileSystem::ApplyLock(int rank, RequestType op, uint64_t file, const LockPacket &lock) {
    dagonfs::LockRequest request;
    request.file = file;
    request.owner = dagonfs::LockOwner{rank, lock.owner,
                                       (lock.flags & DAGONFS_LOCK_FLOCK) ? dagonfs::LockKind::Flock : dagonfs::LockKind::Posix};
    request.type = lock.type;
    request.start = lock.start;
    request.end = lock.end;
    request.pid = lock.pid;
    request.wait = (lock.flags & DAGONFS_LOCK_WAIT) != 0;
    request.token = lock.token;
    switch (op) {
    case LOCK_SET:
        return lockTable.set(request);
    case LOCK_GET:
        return {lockTable.test(request)};
    case LOCK_RELEASE: {
        std::vector<dagonfs::LockReply> replies = lockTable.release(file, request.owner);
        dagonfs::LockReply done;
        done.rank = rank;
        done.token = lock.token;
        replies.push_back(done);
        return replies;
    }
    case LOCK_CANCEL:
        return lockTable.cancel(rank, lock.token, EINTR);
    default:
        return {};
    }
}

void FileSystem::DeliverLockReplies(const std::vector<dagonfs::LockReply> &replies) {
    for (const dagonfs::LockReply &reply : replies) {
        LockPacket packet{};
        packet.token = reply.token;
        packet.error = reply.error;
        packet.type = reply.type;
        packet.start = reply.start;
        packet.end = reply.end;
        packet.pid = reply.pid;
        if (reply.rank == mpiRank) {
            CompleteLockRequest(packet);
        } else {
            RequestSender::sendLockReply(packet, reply.rank);
        }
    }
}

void FileSystem::SubmitLock(fuse_req_t req, RequestType op, uint64_t file, LockPacket lock) {
    const uint64_t token = ++nextLockToken;
    lock.token = token;
    {
        std::lock_guard<std::mutex> guard(pendingLocksMutex);
        pendingLocks[token] = PendingLock{req, op == LOCK_GET};
    }
    if (IsLockCoordinator()) {
        DeliverLockReplies(ApplyLock(mpiRank, op, file, lock));
    } else {
        RequestSender::sendLockRequest(op, file, lock);
    }
    if (op == LOCK_SET && (lock.flags & DAGONFS_LOCK_WAIT)) {
        // Still waiting: let a signal interrupt the wait. The mutex keeps the
        // request alive (no reply can be sent) while the callback is installed.
        std::lock_guard<std::mutex> guard(pendingLocksMutex);
        if (pendingLocks.count(token) != 0) {
            fuse_req_interrupt_func(req, InterruptLock, reinterpret_cast<void *>(static_cast<uintptr_t>(token)));
        }
    }
}

void FileSystem::InterruptLock(fuse_req_t req, void *data) {
    (void) req;
    // Called with libfuse's request lock held, possibly from inside
    // fuse_req_interrupt_func: the request must not be answered here.
    const uint64_t token = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(data));
    std::thread(&FileSystem::CancelLock, token).detach();
}

void FileSystem::CancelLock(uint64_t token) {
    {
        std::lock_guard<std::mutex> guard(pendingLocksMutex);
        if (pendingLocks.count(token) == 0) {
            return; // already answered
        }
    }
    if (IsLockCoordinator()) {
        DeliverLockReplies(lockTable.cancel(mpiRank, token, EINTR));
    } else {
        LockPacket cancel{};
        cancel.token = token;
        RequestSender::sendLockRequest(LOCK_CANCEL, 0, cancel);
    }
}

void FileSystem::ServeLockRequest(int source, const PeerRequest &request) {
    if (!IsLockCoordinator()) {
        return;
    }
    DeliverLockReplies(ApplyLock(source, request.type, request.globalId, request.lock));
}

void FileSystem::CompleteLockRequest(const LockPacket &lock) {
    PendingLock pending{};
    {
        std::lock_guard<std::mutex> guard(pendingLocksMutex);
        const auto entry = pendingLocks.find(lock.token);
        if (entry == pendingLocks.end()) {
            return;
        }
        pending = entry->second;
        pendingLocks.erase(entry);
    }
    if (lock.error != 0) {
        fuse_reply_err(pending.req, lock.error);
        return;
    }
    if (pending.query) {
        struct flock conflict{};
        conflict.l_type = static_cast<short>(lock.type);
        conflict.l_whence = SEEK_SET;
        if (lock.type != F_UNLCK) {
            conflict.l_start = static_cast<off_t>(lock.start);
            conflict.l_len = lock.end == dagonfs::kLockToEnd ? 0 : static_cast<off_t>(lock.end - lock.start + 1);
            conflict.l_pid = static_cast<pid_t>(lock.pid);
        }
        fuse_reply_lock(pending.req, &conflict);
        return;
    }
    fuse_reply_err(pending.req, 0);
}

void FileSystem::FuseGetLock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi, struct flock* lock) {
    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    File *file = dynamic_cast<File *>(inode);
    if (file == nullptr) {
        // Only regular files are locked through the file system.
        lock->l_type = F_UNLCK;
        fuse_reply_lock(req, lock);
        return;
    }
    LockPacket query{};
    if (!lockRange(lock, query.start, query.end)) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    query.owner = fi->lock_owner;
    query.type = lock->l_type;
    query.pid = static_cast<uint32_t>(lock->l_pid);
    SubmitLock(req, LOCK_GET, LockKey(file), query);
}

/**
 * F_SETLK / F_SETLKW (POSIX record locks, also used by lockf). A blocking
 * request stays pending, without blocking the FUSE loop, until the lock is
 * granted, a deadlock is detected (EDEADLK) or a signal interrupts it (EINTR).
 */
void FileSystem::FuseSetLock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi, struct flock* lock, int sleep) {
    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    File *file = dynamic_cast<File *>(inode);
    if (file == nullptr) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    LockPacket request{};
    if (!lockRange(lock, request.start, request.end)) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    if (lock->l_type != F_RDLCK && lock->l_type != F_WRLCK && lock->l_type != F_UNLCK) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    request.owner = fi->lock_owner;
    request.type = lock->l_type;
    request.pid = static_cast<uint32_t>(lock->l_pid);
    request.flags = sleep ? DAGONFS_LOCK_WAIT : 0;
    if (peerToPeer && lock->l_type != F_WRLCK) {
        // Unlocking (or downgrading) lets other peers in: publish the data
        // written under the lock first, as close() does.
        StoreFileContent(file, ino);
    }
    const uint64_t key = LockKey(file);
    if (!IsLockCoordinator() && lock->l_type != F_UNLCK) {
        lockedOwners.insert({key, fi->lock_owner, false});
    }
    SubmitLock(req, LOCK_SET, key, request);
}

/**
 * flock(2). The owner is the open file description, and the lock is released
 * by its last close (FuseRelease with flock_release).
 */
void FileSystem::FuseFlock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi, int op) {
    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    File *file = dynamic_cast<File *>(inode);
    if (file == nullptr) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    LockPacket request{};
    switch (op & ~LOCK_NB) {
    case LOCK_SH: request.type = F_RDLCK; break;
    case LOCK_EX: request.type = F_WRLCK; break;
    case LOCK_UN: request.type = F_UNLCK; break;
    default:
        fuse_reply_err(req, EINVAL);
        return;
    }
    request.owner = fi->lock_owner;
    request.start = 0;
    request.end = dagonfs::kLockToEnd;
    request.pid = static_cast<uint32_t>(fuse_req_ctx(req)->pid);
    request.flags = DAGONFS_LOCK_FLOCK | ((op & LOCK_NB) ? 0 : DAGONFS_LOCK_WAIT);
    if (peerToPeer && request.type != F_WRLCK) {
        StoreFileContent(file, ino);
    }
    const uint64_t key = LockKey(file);
    if (!IsLockCoordinator() && request.type != F_UNLCK) {
        lockedOwners.insert({key, fi->lock_owner, true});
    }
    SubmitLock(req, LOCK_SET, key, request);
}

// ---------------------------------------------------- space and data ranges

/**
 * fallocate(2) and posix_fallocate(3). Memory is the backing store, so
 * allocation only extends the file; hole punching and range zeroing write
 * zeros. Other modes (collapse, insert, unshare) fail with EOPNOTSUPP.
 */
void FileSystem::FuseFallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset, off_t length, struct fuse_file_info* fi) {
    (void) fi;
    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    File *file = dynamic_cast<File *>(inode);
    if (file == nullptr) {
        fuse_reply_err(req, isDirectory(inode) ? EISDIR : ENODEV);
        return;
    }
    if (mode & ~(FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE | FALLOC_FL_ZERO_RANGE)) {
        fuse_reply_err(req, EOPNOTSUPP);
        return;
    }
    const bool punch = (mode & FALLOC_FL_PUNCH_HOLE) != 0;
    const bool zero = (mode & FALLOC_FL_ZERO_RANGE) != 0;
    const bool keepSize = (mode & FALLOC_FL_KEEP_SIZE) != 0;
    if (offset < 0 || length <= 0 || (punch && (zero || !keepSize))) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    if (length > std::numeric_limits<off_t>::max() - offset) {
        fuse_reply_err(req, EFBIG);
        return;
    }
    if (!LoadFileContent(file, ino)) {
        fuse_reply_err(req, ENOMEM);
        return;
    }
    const size_t begin = static_cast<size_t>(offset);
    const size_t end = static_cast<size_t>(offset + length);
    const size_t size = static_cast<size_t>(file->m_fuseEntryParam.attr.st_size);
    const size_t newSize = keepSize ? size : std::max(size, end);
    if (!file->ensureCapacity(std::max(newSize, size))) {
        fuse_reply_err(req, ENOSPC);
        return;
    }
    char *data = static_cast<char *>(file->m_buf);
    bool changed = false;
    if ((punch || zero) && begin < newSize) {
        memset(data + begin, 0, std::min(end, newSize) - begin);
        changed = true;
    }
    if (newSize > size) {
        // The new tail reads back as zeros.
        memset(data + size, 0, newSize - size);
        SetSize(file, newSize);
        changed = true;
    }
    if (changed) {
        file->setWaiting();
        Touch(file, true);
    }
    fuse_reply_err(req, 0);
}

#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 8)
/**
 * lseek(2) with SEEK_DATA / SEEK_HOLE (the kernel resolves the other
 * whence values itself). Files are never sparse: all of [0, size) is data and
 * the only hole is the implicit one at the end of the file.
 */
void FileSystem::FuseLseek(fuse_req_t req, fuse_ino_t ino, off_t off, int whence, struct fuse_file_info* fi) {
    (void) fi;
    INode *inode = GetINode(ino);
    if (inode == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    const off_t size = inode->m_fuseEntryParam.attr.st_size;
    if (whence != SEEK_DATA && whence != SEEK_HOLE) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    if (off < 0 || off >= size) {
        fuse_reply_err(req, ENXIO);
        return;
    }
    fuse_reply_lseek(req, whence == SEEK_DATA ? off : size);
}
#endif

#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 4)
/**
 * copy_file_range(2): copy between the front-end buffers of two open files
 * without a round trip through the caller.
 */
void FileSystem::FuseCopyFileRange(fuse_req_t req, fuse_ino_t ino_in, off_t off_in, struct fuse_file_info* fi_in,
                                   fuse_ino_t ino_out, off_t off_out, struct fuse_file_info* fi_out,
                                   size_t len, int flags) {
    (void) fi_in;
    File *source = dynamic_cast<File *>(GetINode(ino_in));
    File *target = dynamic_cast<File *>(GetINode(ino_out));
    if (source == nullptr || target == nullptr) {
        fuse_reply_err(req, GetINode(ino_in) == nullptr || GetINode(ino_out) == nullptr ? ENOENT : EINVAL);
        return;
    }
    if (flags != 0 || off_in < 0 || off_out < 0) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    if (fi_out != nullptr && (fi_out->flags & O_APPEND)) {
        fuse_reply_err(req, EBADF);
        return;
    }
    if (!LoadFileContent(source, ino_in) || !LoadFileContent(target, ino_out)) {
        fuse_reply_err(req, ENOMEM);
        return;
    }
    const size_t sourceSize = std::min(static_cast<size_t>(source->m_fuseEntryParam.attr.st_size), source->m_capacity);
    const size_t from = static_cast<size_t>(off_in);
    const size_t to = static_cast<size_t>(off_out);
    if (from >= sourceSize || len == 0) {
        fuse_reply_write(req, 0);
        return;
    }
    const size_t count = std::min(len, sourceSize - from);
    if (source == target && from < to + count && to < from + count) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    if (count > static_cast<size_t>(std::numeric_limits<off_t>::max()) - to) {
        fuse_reply_err(req, EFBIG);
        return;
    }
    if (!target->ensureCapacity(to + count)) {
        fuse_reply_err(req, ENOSPC);
        return;
    }
    // ensureCapacity may move the buffer of the target (and of the source when they are the same file).
    memmove(static_cast<char *>(target->m_buf) + to, static_cast<const char *>(source->m_buf) + from, count);
    if (to + count > static_cast<size_t>(target->m_fuseEntryParam.attr.st_size)) {
        SetSize(target, to + count);
    }
    target->setWaiting();
    Touch(target, true);
    setTimeNow(source->m_fuseEntryParam.attr, true, false, false);
    fuse_reply_write(req, count);
}
#endif

/** poll(2) / select(2): a regular file is always ready for reading and writing. */
void FileSystem::FusePoll(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi, struct fuse_pollhandle* ph) {
    (void) fi;
    if (ph != nullptr) {
        // Readiness never changes, so no notification will ever be sent.
        fuse_pollhandle_destroy(ph);
    }
    if (GetINode(ino) == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    fuse_reply_poll(req, POLLIN | POLLOUT | POLLRDNORM | POLLWRNORM);
}

/** ioctl(2): no device-specific requests exist; the kernel's generic ones never get here. */
#if FUSE_USE_VERSION < 35
void FileSystem::FuseIoctl(fuse_req_t req, fuse_ino_t ino, int cmd, void* arg, struct fuse_file_info* fi,
                           unsigned flags, const void* in_buf, size_t in_bufsz, size_t out_bufsz)
#else
void FileSystem::FuseIoctl(fuse_req_t req, fuse_ino_t ino, unsigned int cmd, void* arg, struct fuse_file_info* fi,
                           unsigned flags, const void* in_buf, size_t in_bufsz, size_t out_bufsz)
#endif
{
    (void) cmd; (void) arg; (void) fi; (void) flags; (void) in_buf; (void) in_bufsz; (void) out_bufsz;
    fuse_reply_err(req, GetINode(ino) == nullptr ? ENOENT : ENOTTY);
}

/**
 * Serve a read from the front-end buffer loaded at open time.
 */
void FileSystem::FuseRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info* fi) {
    (void) fi;
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    if (isDirectory(inode_p)) {
        fuse_reply_err(req, EISDIR);
        return;
    }
    File *file_p = dynamic_cast<File *>(inode_p);
    if (file_p == nullptr) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    if (off < 0) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    if (!LoadFileContent(file_p, ino)) {
        fuse_reply_err(req, ENOMEM);
        return;
    }

    // Never read past the buffer, whatever the (concurrently updated) size says.
    const size_t fileSize = std::min(static_cast<size_t>(file_p->m_fuseEntryParam.attr.st_size), file_p->m_capacity);
    const size_t offset = static_cast<size_t>(off);
    setTimeNow(file_p->m_fuseEntryParam.attr, true, false, false);

    // A read at or beyond the end of the file returns no data.
    if (offset >= fileSize || file_p->m_buf == nullptr) {
        fuse_reply_buf(req, nullptr, 0);
        return;
    }
    const size_t bytesRead = std::min(size, fileSize - offset);
    fuse_reply_buf(req, static_cast<const char *>(file_p->m_buf) + offset, bytesRead);
}

/**
 * Apply a write to the front-end buffer; the data reach the ranks on flush.
 */
void FileSystem::FuseWrite(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size, off_t off, struct fuse_file_info* fi) {
    INode *inode_p = GetINode(ino);
    if (inode_p == nullptr) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    if (isDirectory(inode_p)) {
        fuse_reply_err(req, EISDIR);
        return;
    }
    File *file_p = dynamic_cast<File *>(inode_p);
    if (file_p == nullptr) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    if (buf == nullptr || off < 0) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    if (!LoadFileContent(file_p, ino)) {
        fuse_reply_err(req, ENOMEM);
        return;
    }

    size_t offset = static_cast<size_t>(off);
    if (fi != nullptr && (fi->flags & O_APPEND)) {
        offset = static_cast<size_t>(file_p->m_fuseEntryParam.attr.st_size);
    }
    if (size > SIZE_MAX - offset) {
        fuse_reply_err(req, EFBIG);
        return;
    }
    const size_t newSize = offset + size;
    if (!file_p->ensureCapacity(newSize)) {
        fuse_reply_err(req, ENOMEM);
        return;
    }

    memcpy(static_cast<char *>(file_p->m_buf) + offset, buf, size);
    if (newSize > static_cast<size_t>(file_p->m_fuseEntryParam.attr.st_size)) {
        SetSize(file_p, newSize);
    }
    file_p->setWaiting();
    Touch(file_p, true);

    fuse_reply_write(req, size);
}
