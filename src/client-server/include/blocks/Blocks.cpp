//
// Created by frank on 12/30/24.
//

#include "Blocks.hpp"

#include <cstring>

#include "data_blocks_info.hpp"

Blocks *Blocks::instance = nullptr;

Blocks *Blocks::getInstance() {
	if (instance == nullptr) {
		instance = new Blocks();
	}

	return instance;
}

Blocks::Blocks() = default;

Blocks::~Blocks() {
	for (auto& [inode, blocks] : FileSystemDataBlocks) {
		deleteBlocks(blocks);
	}
}

void Blocks::deleteBlocks(vector<DataBlock *> &blocks) {
	for (DataBlock* block : blocks) {
		delete block;
	}
	blocks.clear();
}

void Blocks::replaceLocalBlocks(fuse_ino_t inode, vector<DataBlock *> &&blocks) {
	vector<DataBlock *> previous;
	{
		std::lock_guard<std::mutex> lock(registryMutex);
		if (blocks.empty()) {
			const auto existing = FileSystemDataBlocks.find(inode);
			if (existing != FileSystemDataBlocks.end()) {
				previous.swap(existing->second);
				FileSystemDataBlocks.erase(existing);
			}
		} else {
			previous.swap(FileSystemDataBlocks[inode]);
			FileSystemDataBlocks[inode] = std::move(blocks);
		}
	}
	deleteBlocks(previous);
}

void Blocks::releaseLocalBlocks(fuse_ino_t inode) {
	replaceLocalBlocks(inode, vector<DataBlock *>());
}

vector<void *> Blocks::localBlockAddresses(fuse_ino_t inode, size_t expectedBlocks, unsigned int rank, unsigned int firstIndex) {
	std::lock_guard<std::mutex> lock(registryMutex);
	vector<void *> addresses;
	addresses.reserve(expectedBlocks);
	if (expectedBlocks == 0) {
		return addresses;
	}
	vector<DataBlock *> &slice = FileSystemDataBlocks[inode];
	while (slice.size() < expectedBlocks) {
		DataBlock *padding = new DataBlock(inode, rank, firstIndex + slice.size());
		memset(padding->getData(), 0, FILE_SYSTEM_SINGLE_BLOCK_SIZE);
		slice.push_back(padding);
	}
	for (size_t index = 0; index < expectedBlocks; ++index) {
		addresses.push_back(slice[index]->getData());
	}
	return addresses;
}

bool Blocks::blockListExistForInode(fuse_ino_t inode) const {
	std::lock_guard<std::mutex> lock(registryMutex);
	return FileSystemDataBlocks.find(inode) != FileSystemDataBlocks.end();
}

unsigned int Blocks::getNumberOfUsedBlocksOfInode(fuse_ino_t inode) const {
	std::lock_guard<std::mutex> lock(registryMutex);
	const auto blocks = FileSystemDataBlocks.find(inode);
	return blocks == FileSystemDataBlocks.end() ? 0 : blocks->second.size();
}

size_t Blocks::getTotalBlockBytesOfInode(fuse_ino_t inode) const {
	return static_cast<size_t>(getNumberOfUsedBlocksOfInode(inode)) * FILE_SYSTEM_SINGLE_BLOCK_SIZE;
}

bool Blocks::hasNoBlocks(fuse_ino_t inode) const {
	return getNumberOfUsedBlocksOfInode(inode) == 0;
}
