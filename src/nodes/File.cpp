//
// Created by frank on 11/29/24.
//

#include "File.hpp"

#include <cstdlib>
#include <cstring>

#include "../blocks/data_blocks_info.hpp"

File::File() : m_buf(nullptr), m_capacity(0), m_storedSize(0), m_openCount(0), m_globalId(0), isWaitingForDistributedWrite(false) {
}

File::~File() {
    releaseBuffer();
}

bool File::ensureCapacity(size_t size) {
    if (size <= m_capacity && (m_buf != nullptr || size == 0)) {
        return true;
    }
    const size_t blocks = size / FILE_SYSTEM_SINGLE_BLOCK_SIZE + (size % FILE_SYSTEM_SINGLE_BLOCK_SIZE != 0);
    const size_t newCapacity = (blocks == 0 ? 1 : blocks) * static_cast<size_t>(FILE_SYSTEM_SINGLE_BLOCK_SIZE);
    if (newCapacity / FILE_SYSTEM_SINGLE_BLOCK_SIZE != (blocks == 0 ? 1 : blocks)) {
        return false; // overflow
    }
    void *newBuf = realloc(m_buf, newCapacity);
    if (newBuf == nullptr) {
        return false;
    }
    memset(static_cast<char *>(newBuf) + m_capacity, 0, newCapacity - m_capacity);
    m_buf = newBuf;
    m_capacity = newCapacity;
    return true;
}

void File::adoptBuffer(void *buffer, size_t capacity) {
    releaseBuffer();
    m_buf = buffer;
    m_capacity = buffer == nullptr ? 0 : capacity;
}

void File::releaseBuffer() {
    free(m_buf);
    m_buf = nullptr;
    m_capacity = 0;
}
