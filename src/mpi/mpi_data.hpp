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
 * REMOVE_XATTR) are only sent in the peer-to-peer model; DELETE is only sent
 * in the client-server model.
 */
typedef enum {WRITE, READ, CREATE_FILE, DELETE_FILE, CREATE_DIR, DELETE_DIR, RENAME, TERMINATE, LINK, DELETE,
              SYMLINK, SET_ATTR, SET_XATTR, REMOVE_XATTR} RequestType;

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
} MetadataPacket;

typedef struct PeerRequest {
	RequestType type;
	/** Length in bytes of the first path (target / new object), < DAGONFS_MAX_REQUEST_PATH. */
	uint32_t firstPathLength;
	/** Length in bytes of the second path (RENAME/LINK/SYMLINK destination, xattr name), < DAGONFS_MAX_REQUEST_PATH. */
	uint32_t secondPathLength;
	/** Length in bytes of the value that follows the paths (SET_XATTR), <= DAGONFS_MAX_REQUEST_VALUE. */
	uint32_t valueLength;
	/** CREATE_FILE: cluster-wide identity, ((creator rank + 1) << 48) | creator inode. */
	uint64_t globalId;
	/** WRITE / READ: parameters of the data-plane transfer that follows; DELETE: io.inode. */
	IORequestPacket io;
	/** SET_ATTR / SET_XATTR. */
	MetadataPacket meta;
} PeerRequest;

#endif //MPI_DATA_HPP
