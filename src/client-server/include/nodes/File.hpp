//
// Created by frank on 11/29/24.
//

#ifndef FILE_HPP
#define FILE_HPP

#include <cstddef>
#include <cstdint>

#include "inodes_data_structures.hpp"

/**
 * @brief A regular file.
 *
 * While at least one descriptor is open, the whole content lives in the
 * front-end buffer @ref m_buf. The distributed copy (the blocks scattered over
 * the MPI ranks) holds @ref m_storedSize bytes and is refreshed on flush,
 * fsync and the last release whenever the buffer is dirty.
 */
class File final: public INode {
public:
    /** Front-end copy of the content, or nullptr when not loaded. */
    void *m_buf;
    /** Allocated bytes of @ref m_buf (always a multiple of the block size). */
    size_t m_capacity;
    /** Size, in bytes, of the copy currently distributed over the ranks. */
    size_t m_storedSize;
    /** Number of open file handles referring to this inode. */
    unsigned int m_openCount;
    /** Cluster-wide identity (peer-to-peer model only; 0 when unused). */
    uint64_t m_globalId;
    /** True when @ref m_buf differs from the distributed copy. */
    bool isWaitingForDistributedWrite;

public:
    File();
    ~File() override;

    void *getData(){return m_buf;};
    bool isWaitingForWriting() { return isWaitingForDistributedWrite; };
    void setWaiting(){isWaitingForDistributedWrite = true;};
    void removeWaiting(){isWaitingForDistributedWrite = false;};

    /**
     * @brief Grow @ref m_buf to hold at least @p size bytes, zero-filling new memory.
     * @return false if the allocation failed (the buffer is left untouched).
     */
    bool ensureCapacity(size_t size);
    /** @brief Adopt a buffer of @p capacity bytes produced by a distributed read. */
    void adoptBuffer(void *buffer, size_t capacity);
    /** @brief Free the front-end buffer. */
    void releaseBuffer();
};

#endif //FILE_HPP
