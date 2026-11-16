#ifndef DAGONFS_NATIVE_PROTOCOL_H
#define DAGONFS_NATIVE_PROTOCOL_H

#include <stdint.h>

#define DAGONFS_NATIVE_MAGIC 0x44474653u
#define DAGONFS_NATIVE_VERSION 1u
#define DAGONFS_NATIVE_MAX_PATH 4096u
#define DAGONFS_NATIVE_MAX_IO (8u * 1024u * 1024u)

enum dagonfs_native_opcode {
    DAGONFS_NATIVE_OPEN = 1,
    DAGONFS_NATIVE_CLOSE,
    DAGONFS_NATIVE_READ,
    DAGONFS_NATIVE_WRITE,
    DAGONFS_NATIVE_FSYNC,
    DAGONFS_NATIVE_STAT,
    DAGONFS_NATIVE_MKDIR,
    DAGONFS_NATIVE_UNLINK,
    DAGONFS_NATIVE_RMDIR,
    DAGONFS_NATIVE_RENAME,
    DAGONFS_NATIVE_LIST,
    DAGONFS_NATIVE_SHUTDOWN
};

struct dagonfs_native_request {
    uint32_t magic;
    uint16_t version;
    uint16_t opcode;
    uint32_t path_length;
    uint32_t second_path_length;
    uint32_t data_length;
    int32_t flags;
    uint32_t mode;
    int64_t handle;
    int64_t offset;
};

struct dagonfs_native_reply {
    uint32_t magic;
    uint16_t version;
    uint16_t reserved;
    int32_t error;
    uint32_t data_length;
    int64_t value;
    uint64_t inode;
    uint64_t size;
    uint32_t mode;
    uint32_t nlink;
    uint32_t uid;
    uint32_t gid;
    int64_t atime_sec;
    int64_t mtime_sec;
    int64_t ctime_sec;
};

#endif
