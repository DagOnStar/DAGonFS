#ifndef DAGONFS_NATIVE_FRONTEND_HPP
#define DAGONFS_NATIVE_FRONTEND_HPP

#include <cstdint>
#include <string>
#include "../../utils/fuse_headers.hpp"
class INode;

namespace dagonfs {

/** FUSE-free pathname service. Rank zero serves a versioned Unix-socket protocol. */
class NativeFrontend {
public:
    NativeFrontend(int rank, int worldSize);
    int serve(const std::string &socketPath);
private:
    bool serveRequest(int client, bool &running);
    static void closeHandle(int64_t handle);
    static int makeDirectory(const std::string &path, uint32_t mode);
    static int removeEntry(const std::string &path, bool directory);
    static int renameEntry(const std::string &from, const std::string &to);
public:
    static int validateName(const std::string &name);
    static INode *resolve(const std::string &path, fuse_ino_t *number = nullptr);
};

} // namespace dagonfs
#endif
