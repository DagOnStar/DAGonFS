#ifndef DAGONFS_NATIVE_FRONTEND_HPP
#define DAGONFS_NATIVE_FRONTEND_HPP

#include <string>

namespace dagonfs {

/** FUSE-free pathname service. Rank zero serves a versioned Unix-socket protocol. */
class NativeFrontend {
public:
    NativeFrontend(int rank, int worldSize);
    int serve(const std::string &socketPath);
};

} // namespace dagonfs
#endif
