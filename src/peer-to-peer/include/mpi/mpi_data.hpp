//
// Created by frank on 12/30/24.
//

#ifndef MPI_DATA_HPP
#define MPI_DATA_HPP

#include <cstddef>
#include <cstdint>
#include "../utils/fuse_headers.hpp"

/**
 * Peer-to-peer control messages.
 *
 * A peer that originates an operation sends ONE message (tag 0) to each other
 * peer: a fixed PeerRequest header immediately followed by the bytes of up to
 * two paths (no terminating NUL; lengths in the header). Receivers size their
 * buffer with MPI_Probe/MPI_Get_count. Messages contain only value types:
 * process-local addresses are never transmitted.
 */
typedef enum {WRITE, READ, CREATE_FILE, DELETE_FILE, CREATE_DIR, DELETE_DIR, RENAME, TERMINATE, LINK} RequestType;

/** Maximum length of a replicated path, including the terminating NUL. */
constexpr size_t DAGONFS_MAX_REQUEST_PATH = 4096;

typedef struct IORequestPacket {
	/** Cluster-wide identity of the file (not a local inode number). */
	fuse_ino_t inode;
	size_t fileSize;
	size_t reqSize;
	off_t offset;
} IORequestPacket;

typedef struct PeerRequest {
	RequestType type;
	/** Length in bytes of the first path (target / new object), < DAGONFS_MAX_REQUEST_PATH. */
	uint32_t firstPathLength;
	/** Length in bytes of the second path (RENAME/LINK destination), < DAGONFS_MAX_REQUEST_PATH. */
	uint32_t secondPathLength;
	/** CREATE_FILE: cluster-wide identity, ((creator rank + 1) << 48) | creator inode. */
	uint64_t globalId;
	/** WRITE / READ: parameters of the data-plane collective that follows. */
	IORequestPacket io;
} PeerRequest;

#endif //MPI_DATA_HPP
