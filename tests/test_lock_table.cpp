// Unit tests of the cluster-wide lock table (no FUSE, no MPI, no mount).
// Built and run by ctest as dagonfs-lock-table.

#include "../src/locks/LockTable.hpp"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include <fcntl.h>

using namespace dagonfs;

namespace {

int failures = 0;

void check(bool condition, const char *what, int line) {
    if (!condition) {
        std::fprintf(stderr, "line %d: %s\n", line, what);
        ++failures;
    }
}

#define CHECK(condition) check((condition), #condition, __LINE__)

LockRequest posix(int32_t rank, uint64_t owner, int type, int64_t start, int64_t end, bool wait = false,
                  uint64_t token = 1, uint64_t file = 7) {
    LockRequest request;
    request.file = file;
    request.owner = LockOwner{rank, owner, LockKind::Posix};
    request.type = type;
    request.start = start;
    request.end = end;
    request.pid = static_cast<uint32_t>(100 + owner);
    request.wait = wait;
    request.token = token;
    return request;
}

LockRequest flockRequest(int32_t rank, uint64_t owner, int type, bool wait = false, uint64_t token = 1) {
    LockRequest request = posix(rank, owner, type, 0, kLockToEnd, wait, token);
    request.owner.kind = LockKind::Flock;
    return request;
}

const LockReply *find(const std::vector<LockReply> &replies, int32_t rank, uint64_t token) {
    for (const LockReply &reply : replies) {
        if (reply.rank == rank && reply.token == token) return &reply;
    }
    return nullptr;
}

void sharedAndExclusive() {
    LockTable table;
    auto replies = table.set(posix(0, 1, F_RDLCK, 0, 99));
    CHECK(replies.size() == 1 && replies[0].error == 0);
    // Readers share, a writer conflicts.
    CHECK(table.set(posix(1, 2, F_RDLCK, 50, 149))[0].error == 0);
    CHECK(table.set(posix(1, 3, F_WRLCK, 90, 90))[0].error == EAGAIN);
    // Disjoint ranges never conflict.
    CHECK(table.set(posix(1, 3, F_WRLCK, 150, 199))[0].error == 0);
    const LockReply probe = table.test(posix(2, 4, F_WRLCK, 0, kLockToEnd));
    CHECK(probe.type == F_RDLCK && probe.start == 0 && probe.end == 99 && probe.pid == 101);
    // The owner's own locks never conflict with its requests.
    CHECK(table.test(posix(0, 1, F_WRLCK, 0, 10)).type == F_UNLCK);
}

void splitAndCoalesce() {
    LockTable table;
    table.set(posix(0, 1, F_WRLCK, 0, 99));
    table.set(posix(0, 1, F_UNLCK, 40, 59));
    CHECK(table.heldCount(7) == 2);
    CHECK(table.test(posix(0, 2, F_WRLCK, 40, 59)).type == F_UNLCK);
    CHECK(table.test(posix(0, 2, F_WRLCK, 60, 60)).type == F_WRLCK);
    table.set(posix(0, 1, F_WRLCK, 40, 59));
    CHECK(table.heldCount(7) == 1);
    // Downgrade part of the range in place.
    table.set(posix(0, 1, F_RDLCK, 0, 9));
    CHECK(table.heldCount(7) == 2);
    CHECK(table.set(posix(0, 2, F_RDLCK, 0, 9))[0].error == 0);
    table.set(posix(0, 1, F_UNLCK, 0, kLockToEnd));
    table.set(posix(0, 2, F_UNLCK, 0, kLockToEnd));
    CHECK(table.heldCount(7) == 0);
}

void waitersAreGrantedOnRelease() {
    LockTable table;
    table.set(posix(0, 1, F_WRLCK, 0, kLockToEnd));
    auto replies = table.set(posix(1, 2, F_WRLCK, 0, 0, true, 21));
    CHECK(replies.empty());
    replies = table.set(posix(2, 3, F_RDLCK, 5, 5, true, 31));
    CHECK(replies.empty());
    CHECK(table.waiterCount(7) == 2);
    // Closing the file releases every lock of the owner and grants both waiters (disjoint ranges).
    replies = table.release(7, LockOwner{0, 1, LockKind::Posix});
    CHECK(replies.size() == 2);
    CHECK(find(replies, 1, 21) != nullptr && find(replies, 1, 21)->error == 0);
    CHECK(find(replies, 2, 31) != nullptr && find(replies, 2, 31)->error == 0);
    CHECK(table.waiterCount(7) == 0);
}

void cancelledWaiterGetsEintr() {
    LockTable table;
    table.set(posix(0, 1, F_WRLCK, 0, kLockToEnd));
    CHECK(table.set(posix(1, 2, F_WRLCK, 0, 0, true, 5)).empty());
    auto replies = table.cancel(1, 5, EINTR);
    CHECK(replies.size() == 1 && replies[0].error == EINTR);
    CHECK(table.cancel(1, 5, EINTR).empty());
    CHECK(table.waiterCount(7) == 0);
}

void deadlockIsDetected() {
    LockTable table;
    table.set(posix(0, 1, F_WRLCK, 0, 0));
    table.set(posix(1, 2, F_WRLCK, 10, 10));
    // Owner 1 waits for owner 2 ...
    CHECK(table.set(posix(0, 1, F_WRLCK, 10, 10, true, 11)).empty());
    // ... so owner 2 waiting for owner 1 would deadlock.
    auto replies = table.set(posix(1, 2, F_WRLCK, 0, 0, true, 22));
    CHECK(replies.size() == 1 && replies[0].error == EDEADLK);
    // Across files too.
    LockTable other;
    other.set(posix(0, 1, F_WRLCK, 0, 0, false, 1, 1));
    other.set(posix(1, 2, F_WRLCK, 0, 0, false, 1, 2));
    CHECK(other.set(posix(0, 1, F_WRLCK, 0, 0, true, 3, 2)).empty());
    CHECK(other.set(posix(1, 2, F_WRLCK, 0, 0, true, 4, 1))[0].error == EDEADLK);
}

void flockLocks() {
    LockTable table;
    CHECK(table.set(flockRequest(0, 1, F_RDLCK))[0].error == 0);
    CHECK(table.set(flockRequest(1, 2, F_RDLCK))[0].error == 0);
    CHECK(table.set(flockRequest(1, 3, F_WRLCK))[0].error == EAGAIN);
    // flock and POSIX locks are independent.
    CHECK(table.set(posix(1, 3, F_WRLCK, 0, kLockToEnd))[0].error == 0);
    CHECK(table.set(flockRequest(1, 3, F_WRLCK, true, 9)).empty());
    table.release(7, LockOwner{0, 1, LockKind::Flock});
    auto replies = table.release(7, LockOwner{1, 2, LockKind::Flock});
    CHECK(replies.size() == 1 && replies[0].token == 9 && replies[0].error == 0);
}

void departedRankReleasesLocks() {
    LockTable table;
    table.set(posix(3, 1, F_WRLCK, 0, kLockToEnd));
    CHECK(table.set(posix(0, 2, F_WRLCK, 0, kLockToEnd, true, 8)).empty());
    auto replies = table.releaseRank(3);
    CHECK(replies.size() == 1 && replies[0].rank == 0 && replies[0].token == 8);
}

void invalidRequests() {
    LockTable table;
    CHECK(table.set(posix(0, 1, F_WRLCK, 10, 5))[0].error == EINVAL);
    CHECK(table.set(posix(0, 1, 12345, 0, 5))[0].error == EINVAL);
}

} // namespace

int main() {
    sharedAndExclusive();
    splitAndCoalesce();
    waitersAreGrantedOnRelease();
    cancelledWaiterGetsEintr();
    deadlockIsDetected();
    flockLocks();
    departedRankReleasesLocks();
    invalidRequests();
    if (failures != 0) {
        std::fprintf(stderr, "%d check(s) failed\n", failures);
        return EXIT_FAILURE;
    }
    std::puts("lock table: all checks passed");
    return EXIT_SUCCESS;
}
