//
// Created by frank on 12/30/24.
//

#ifndef MPI_DATA_HPP
#define MPI_DATA_HPP

#include <cstddef>
#include "../utils/fuse_headers.hpp"

/**
 * Control messages broadcast by the master (rank 0) to the storage ranks.
 * Every request is a RequestPacket, followed by an IORequestPacket for the
 * data-plane operations (WRITE, READ, DELETE).
 */
typedef enum {WRITE, READ, CHANGE_DIR, REDUCE_BLOCKS, TERMINATE, DELETE} RequestType;

typedef struct RequstPacket {
	RequestType type;
} RequestPacket;

/**
 * Parameters of a data-plane operation. Only value types are transmitted:
 * process-local addresses never leave the process that owns them.
 */
typedef struct IORequestPacket {
	fuse_ino_t inode;
	size_t fileSize;
	size_t reqSize;
	off_t offset;
} IORequestPacket;

#endif //MPI_DATA_HPP
