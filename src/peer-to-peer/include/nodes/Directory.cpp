//
// Created by frank on 11/29/24.
//

#include "Directory.hpp"

#include <iostream>
using namespace std;

Directory::Directory() {
    dir_name = "";
    parentDir = nullptr;
    m_children = map<string, fuse_ino_t>();
}


/**
 Returns a child inode number given the name of the child.

 @param name The child file / dir name.
 @return The child inode number if the child is found. -1 otherwise.
 */
fuse_ino_t Directory::ChildINodeNumberWithName(const std::string& name) {
    if (m_children.find(name) == m_children.end()) {
        return -1;
    }

    return m_children[name];
}

/**
 Changes the inode number of the child with the given name.

 @param name The name of the child to update.
 @param ino The new inode number.
 @return The old inode number before the change.
 */
fuse_ino_t Directory::UpdateChild(const std::string& name, fuse_ino_t ino) {
    const auto child = m_children.find(name);
    if (child == m_children.end()) {
        return static_cast<fuse_ino_t>(-1);
    }
    fuse_ino_t ino_ret = child->second;
    child->second = ino;

    // TODO: What about directory sizes? Shouldn't we increase the reported size of our dir?

    return ino_ret;
}

fuse_ino_t Directory::DeleteChild(const std::string& name) {
    const auto child = m_children.find(name);
    if (child == m_children.end()) {
        return static_cast<fuse_ino_t>(-1);
    }
    fuse_ino_t ino_ret = child->second;
    m_children.erase(child);
    return ino_ret;
}

/**
 Check if the directory is empty.

 @return True if the only children are the following directories: "." and ".."
 */
bool Directory::hasChildren() {
    map<std::string, fuse_ino_t>::iterator it;
    for (it = m_children.begin(); it != m_children.end(); it++) {
        if (it->first != "." && it->first != "..")
            return true;
    }
    return false;
}
