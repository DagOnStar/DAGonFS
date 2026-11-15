//
// Cluster-wide advisory locks (POSIX record locks and BSD flock locks).
//
// The table is pure bookkeeping, with no FUSE or MPI dependency: the FUSE
// handlers and the control plane translate their requests into LockRequest
// values and deliver the resulting LockReply values to the requesters.
//
// One table serves a whole volume. It lives on the client-server front end
// (rank 0), and on rank 0 in the peer-to-peer model, where the other peers
// reach it through the control plane (see docs/PROTOCOL.md).
//

#ifndef DAGONFS_LOCKTABLE_HPP
#define DAGONFS_LOCKTABLE_HPP

#include <compare>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <vector>

namespace dagonfs {

/** Last byte of a lock that extends to the end of the file (the kernel's OFFSET_MAX). */
constexpr int64_t kLockToEnd = INT64_MAX;

/** POSIX record locks and flock locks never conflict with each other (as on Linux). */
enum class LockKind : uint32_t { Posix = 0, Flock = 1 };

/** Lock types: the values of F_RDLCK, F_WRLCK and F_UNLCK are passed through unchanged. */
struct LockOwner {
    int32_t rank = 0;      ///< rank whose kernel issued the request
    uint64_t owner = 0;    ///< FUSE lock owner (a process for POSIX locks, an open file for flock)
    LockKind kind = LockKind::Posix;
    auto operator<=>(const LockOwner &) const = default;
};

struct LockRequest {
    uint64_t file = 0;     ///< cluster-wide file identity
    LockOwner owner;
    int type = 0;          ///< F_RDLCK, F_WRLCK or F_UNLCK
    int64_t start = 0;     ///< first byte
    int64_t end = kLockToEnd; ///< last byte (inclusive)
    uint32_t pid = 0;
    bool wait = false;     ///< F_SETLKW / flock without LOCK_NB
    uint64_t token = 0;    ///< requester's identifier of the pending FUSE request
};

/** Outcome of a request, addressed to (rank, token). */
struct LockReply {
    int32_t rank = 0;
    uint64_t token = 0;
    int error = 0;         ///< 0, EAGAIN, EDEADLK or EINTR
    /** F_GETLK: the conflicting lock, or type F_UNLCK. */
    int type = 0;
    int64_t start = 0;
    int64_t end = 0;
    uint32_t pid = 0;
};

class LockTable {
public:
    /**
     * F_SETLK / F_SETLKW / flock. Returns the replies to deliver: the
     * request's own reply (unless it has been queued to wait) and the replies
     * of waiters that the change has granted.
     */
    std::vector<LockReply> set(const LockRequest &request);
    /** F_GETLK: the first lock that would prevent @p request, or F_UNLCK. */
    LockReply test(const LockRequest &request) const;
    /** Remove every lock of @p owner on @p file (close, flock release); returns granted waiters. */
    std::vector<LockReply> release(uint64_t file, const LockOwner &owner);
    /** Remove every lock and waiter of @p rank (a peer that went away); returns granted waiters. */
    std::vector<LockReply> releaseRank(int32_t rank);
    /** Withdraw a waiting request; its reply carries @p error. Empty if it is no longer waiting. */
    std::vector<LockReply> cancel(int32_t rank, uint64_t token, int error);

    /** Number of held lock ranges on @p file (tests and diagnostics). */
    size_t heldCount(uint64_t file) const;
    /** Number of requests waiting on @p file (tests and diagnostics). */
    size_t waiterCount(uint64_t file) const;

private:
    struct Held {
        LockOwner owner;
        int type;
        int64_t start;
        int64_t end;
        uint32_t pid;
    };
    struct FileLocks {
        std::vector<Held> held;
        std::deque<LockRequest> waiters;
    };

    mutable std::mutex m_mutex;
    std::map<uint64_t, FileLocks> m_files;

    static const Held *conflict(const FileLocks &locks, const LockRequest &request);
    static void apply(FileLocks &locks, const LockRequest &request);
    bool wouldDeadlock(const LockRequest &request) const;
    void grantWaiters(uint64_t file, std::vector<LockReply> &replies);
    void forget(uint64_t file);
};

} // namespace dagonfs

#endif // DAGONFS_LOCKTABLE_HPP
