//
// Created by frank on 12/30/24.
//

#ifndef MPI_DATA_HPP
#define MPI_DATA_HPP

#include <cstddef>
#include <cstdint>
#include "../utils/fuse_headers.hpp"

/**
 * Control messages, common to both deployment models.
 *
 * The rank that originates an operation (rank 0 in the client-server model,
 * any peer in the peer-to-peer model) sends ONE message (tag 0) to each other
 * rank: a fixed PeerRequest header immediately followed by the bytes of up to
 * two paths and of an optional value (no terminating NULs; lengths in the
 * header). Receivers size their buffer with MPI_Iprobe/MPI_Get_count. Messages
 * contain only value types: process-local addresses are never transmitted.
 *
 * Namespace and metadata requests (CREATE_FILE ... LINK, SYMLINK ...
 * REMOVE_XATTR, MKNOD) and lock requests (LOCK_*) are only sent in the
 * peer-to-peer model; DELETE is only sent in the client-server model.
 *
 * Lock requests go from a peer to the lock coordinator (rank 0), which answers
 * each of them, possibly much later (a waiting lock), with one LOCK_REPLY.
 */
typedef enum {WRITE, READ, CREATE_FILE, DELETE_FILE, CREATE_DIR, DELETE_DIR, RENAME, TERMINATE, LINK, DELETE,
              SYMLINK, SET_ATTR, SET_XATTR, REMOVE_XATTR, MKNOD,
              LOCK_SET, LOCK_GET, LOCK_RELEASE, LOCK_CANCEL, LOCK_REPLY} RequestType;

/** Rank that holds the lock table of a volume. */
constexpr int DAGONFS_LOCK_COORDINATOR = 0;

/** Message tags: requests go to the listener; data and acknowledgements to the transfer's two ends. */
constexpr int DAGONFS_TAG_REQUEST = 0;  ///< PeerRequest, originator -> listener
constexpr int DAGONFS_TAG_STORE = 1;    ///< block slice, originator -> rank (WRITE)
constexpr int DAGONFS_TAG_LOAD = 2;     ///< block slice, rank -> originator (READ)
constexpr int DAGONFS_TAG_ACK = 3;      ///< empty, rank -> originator: WRITE applied

/** Maximum length of a replicated path, including the terminating NUL. */
constexpr size_t DAGONFS_MAX_REQUEST_PATH = 4096;
/** Maximum length of a replicated extended-attribute value (Linux XATTR_SIZE_MAX). */
constexpr size_t DAGONFS_MAX_REQUEST_VALUE = 65536;

/** SET_ATTR: which attributes MetadataPacket carries. */
constexpr uint32_t DAGONFS_ATTR_MODE = 1u << 0;
constexpr uint32_t DAGONFS_ATTR_UID = 1u << 1;
constexpr uint32_t DAGONFS_ATTR_GID = 1u << 2;
constexpr uint32_t DAGONFS_ATTR_ATIME = 1u << 3;
constexpr uint32_t DAGONFS_ATTR_MTIME = 1u << 4;

typedef struct IORequestPacket {
	/** Cluster-wide identity of the file (not a local inode number). */
	fuse_ino_t inode;
	size_t fileSize;
	size_t reqSize;
	off_t offset;
} IORequestPacket;

/**
 * SET_ATTR / SET_XATTR: attribute values as applied by the originator. Times
 * are absolute (a "now" request is resolved once, on the originator), so all
 * peers store identical timestamps.
 */
typedef struct MetadataPacket {
	uint32_t mask;          ///< DAGONFS_ATTR_* bits
	uint32_t mode;          ///< permission bits (07777)
	uint32_t uid;
	uint32_t gid;
	int64_t atimeSeconds;
	int64_t atimeNanoseconds;
	int64_t mtimeSeconds;
	int64_t mtimeNanoseconds;
	int32_t xattrFlags;     ///< SET_XATTR: XATTR_CREATE / XATTR_REPLACE as requested
	uint32_t fileType;      ///< MKNOD: S_IFIFO, S_IFSOCK, S_IFCHR or S_IFBLK
	uint64_t rdev;          ///< MKNOD: device number
} MetadataPacket;

/** LockPacket.flags */
constexpr uint32_t DAGONFS_LOCK_FLOCK = 1u << 0;  ///< flock lock (otherwise a POSIX record lock)
constexpr uint32_t DAGONFS_LOCK_WAIT = 1u << 1;   ///< wait for the lock (F_SETLKW, flock without LOCK_NB)

/**
 * LOCK_*: an advisory lock request on the file PeerRequest.globalId, or its
 * reply. The owner is (sending rank, owner); the token identifies the
 * requester's pending FUSE request and is echoed in the LOCK_REPLY.
 */
typedef struct LockPacket {
	uint64_t owner;         ///< FUSE lock owner on the requesting rank
	uint64_t token;         ///< requester's identifier of the pending request
	int64_t start;          ///< first byte
	int64_t end;            ///< last byte, inclusive (INT64_MAX: to the end of the file)
	uint32_t pid;           ///< locking process (as seen by the holder's kernel)
	int32_t type;           ///< F_RDLCK, F_WRLCK or F_UNLCK
	uint32_t flags;         ///< DAGONFS_LOCK_*
	int32_t error;          ///< LOCK_REPLY: 0 or a POSIX error
} LockPacket;

typedef struct PeerRequest {
	RequestType type;
	/** Length in bytes of the first path (target / new object), < DAGONFS_MAX_REQUEST_PATH. */
	uint32_t firstPathLength;
	/** Length in bytes of the second path (RENAME/LINK/SYMLINK destination, xattr name), < DAGONFS_MAX_REQUEST_PATH. */
	uint32_t secondPathLength;
	/** Length in bytes of the value that follows the paths (SET_XATTR), <= DAGONFS_MAX_REQUEST_VALUE. */
	uint32_t valueLength;
	/** CREATE_FILE, LOCK_*: cluster-wide identity, ((creator rank + 1) << 48) | creator inode. */
	uint64_t globalId;
	/** WRITE / READ: parameters of the data-plane transfer that follows; DELETE: io.inode. */
	IORequestPacket io;
	/** SET_ATTR / SET_XATTR / MKNOD. */
	MetadataPacket meta;
	/** LOCK_* (the file is globalId). */
	LockPacket lock;
} PeerRequest;

#endif //MPI_DATA_HPP
