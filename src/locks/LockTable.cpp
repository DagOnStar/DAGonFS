//
// Cluster-wide advisory locks: see LockTable.hpp.
//

#include "LockTable.hpp"

#include <algorithm>
#include <cerrno>
#include <set>

#include <fcntl.h>

namespace dagonfs {

namespace {

bool overlaps(int64_t start, int64_t end, int64_t otherStart, int64_t otherEnd) {
    return start <= otherEnd && otherStart <= end;
}

LockReply replyTo(const LockRequest &request, int error) {
    LockReply reply;
    reply.rank = request.owner.rank;
    reply.token = request.token;
    reply.error = error;
    reply.type = F_UNLCK;
    return reply;
}

} // namespace

const LockTable::Held *LockTable::conflict(const FileLocks &locks, const LockRequest &request) {
    if (request.type == F_UNLCK) {
        return nullptr;
    }
    for (const Held &held : locks.held) {
        if (held.owner == request.owner || held.owner.kind != request.owner.kind) {
            continue;
        }
        if (!overlaps(held.start, held.end, request.start, request.end)) {
            continue;
        }
        if (request.type == F_WRLCK || held.type == F_WRLCK) {
            return &held;
        }
    }
    return nullptr;
}

void LockTable::apply(FileLocks &locks, const LockRequest &request) {
    // The owner's locks in the range are replaced (POSIX: a new lock converts
    // or splits the owner's existing locks; F_UNLCK removes them).
    std::vector<Held> kept;
    std::vector<Held> own;
    kept.reserve(locks.held.size() + 2);
    for (const Held &held : locks.held) {
        if (held.owner != request.owner) {
            kept.push_back(held);
            continue;
        }
        if (!overlaps(held.start, held.end, request.start, request.end)) {
            own.push_back(held);
            continue;
        }
        if (held.start < request.start) {
            Held before = held;
            before.end = request.start - 1;
            own.push_back(before);
        }
        if (held.end > request.end) {
            Held after = held;
            after.start = request.end + 1;
            own.push_back(after);
        }
    }
    if (request.type != F_UNLCK) {
        own.push_back(Held{request.owner, request.type, request.start, request.end, request.pid});
    }
    // Coalesce adjacent or overlapping ranges of the same type, so that
    // F_GETLK reports whole locks and the table stays small.
    std::sort(own.begin(), own.end(), [](const Held &a, const Held &b) {
        return a.type != b.type ? a.type < b.type : a.start < b.start;
    });
    for (const Held &held : own) {
        if (!kept.empty()) {
            Held &last = kept.back();
            if (last.owner == held.owner && last.type == held.type &&
                (last.end == kLockToEnd || held.start <= last.end + 1)) {
                last.end = std::max(last.end, held.end);
                last.pid = held.pid;
                continue;
            }
        }
        kept.push_back(held);
    }
    locks.held = std::move(kept);
}

bool LockTable::wouldDeadlock(const LockRequest &request) const {
    // Only POSIX locks detect deadlocks (as on Linux). The request would wait
    // for the owners of the conflicting locks; it deadlocks if one of them is
    // itself waiting, directly or transitively, for the requester.
    if (request.owner.kind != LockKind::Posix) {
        return false;
    }
    std::vector<LockOwner> pending;
    std::set<LockOwner> visited;
    const auto blockers = [&](uint64_t file, const LockRequest &waiter) {
        const auto entry = m_files.find(file);
        if (entry == m_files.end()) {
            return;
        }
        for (const Held &held : entry->second.held) {
            if (held.owner != waiter.owner && held.owner.kind == waiter.owner.kind &&
                overlaps(held.start, held.end, waiter.start, waiter.end) &&
                (waiter.type == F_WRLCK || held.type == F_WRLCK)) {
                pending.push_back(held.owner);
            }
        }
    };
    blockers(request.file, request);
    while (!pending.empty()) {
        const LockOwner owner = pending.back();
        pending.pop_back();
        if (owner == request.owner) {
            return true;
        }
        if (!visited.insert(owner).second) {
            continue;
        }
        for (const auto &[file, locks] : m_files) {
            for (const LockRequest &waiter : locks.waiters) {
                if (waiter.owner == owner) {
                    blockers(file, waiter);
                }
            }
        }
    }
    return false;
}

void LockTable::grantWaiters(uint64_t file, std::vector<LockReply> &replies) {
    const auto entry = m_files.find(file);
    if (entry == m_files.end()) {
        return;
    }
    FileLocks &locks = entry->second;
    // A grant can never unblock an earlier waiter, but a downgrade or a
    // release can unblock several: scan until nothing changes.
    bool granted = true;
    while (granted) {
        granted = false;
        for (auto waiter = locks.waiters.begin(); waiter != locks.waiters.end(); ++waiter) {
            if (conflict(locks, *waiter) == nullptr) {
                apply(locks, *waiter);
                replies.push_back(replyTo(*waiter, 0));
                locks.waiters.erase(waiter);
                granted = true;
                break;
            }
        }
    }
}

void LockTable::forget(uint64_t file) {
    const auto entry = m_files.find(file);
    if (entry != m_files.end() && entry->second.held.empty() && entry->second.waiters.empty()) {
        m_files.erase(entry);
    }
}

std::vector<LockReply> LockTable::set(const LockRequest &request) {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::vector<LockReply> replies;
    if (request.start < 0 || request.end < request.start ||
        (request.type != F_RDLCK && request.type != F_WRLCK && request.type != F_UNLCK)) {
        replies.push_back(replyTo(request, EINVAL));
        return replies;
    }
    FileLocks &locks = m_files[request.file];
    // flock conversions are not atomic (as on Linux): the old lock is dropped
    // before the new one is requested, which avoids conversion deadlocks.
    if (request.owner.kind == LockKind::Flock && request.type != F_UNLCK) {
        LockRequest unlock = request;
        unlock.type = F_UNLCK;
        apply(locks, unlock);
        grantWaiters(request.file, replies);
    }
    if (conflict(locks, request) != nullptr) {
        if (!request.wait) {
            replies.push_back(replyTo(request, EAGAIN));
        } else if (wouldDeadlock(request)) {
            replies.push_back(replyTo(request, EDEADLK));
        } else {
            locks.waiters.push_back(request);
        }
        forget(request.file);
        return replies;
    }
    apply(locks, request);
    replies.push_back(replyTo(request, 0));
    grantWaiters(request.file, replies);
    forget(request.file);
    return replies;
}

LockReply LockTable::test(const LockRequest &request) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    LockReply reply = replyTo(request, 0);
    const auto entry = m_files.find(request.file);
    if (entry == m_files.end()) {
        return reply;
    }
    if (const Held *held = conflict(entry->second, request)) {
        reply.type = held->type;
        reply.start = held->start;
        reply.end = held->end;
        reply.pid = held->pid;
    }
    return reply;
}

std::vector<LockReply> LockTable::release(uint64_t file, const LockOwner &owner) {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::vector<LockReply> replies;
    const auto entry = m_files.find(file);
    if (entry == m_files.end()) {
        return replies;
    }
    auto &held = entry->second.held;
    const auto removed = std::remove_if(held.begin(), held.end(), [&](const Held &h) { return h.owner == owner; });
    if (removed != held.end()) {
        held.erase(removed, held.end());
        grantWaiters(file, replies);
    }
    forget(file);
    return replies;
}

std::vector<LockReply> LockTable::releaseRank(int32_t rank) {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::vector<LockReply> replies;
    std::vector<uint64_t> files;
    for (auto &[file, locks] : m_files) {
        std::erase_if(locks.held, [&](const Held &h) { return h.owner.rank == rank; });
        std::erase_if(locks.waiters, [&](const LockRequest &w) { return w.owner.rank == rank; });
        files.push_back(file);
    }
    for (const uint64_t file : files) {
        grantWaiters(file, replies);
        forget(file);
    }
    return replies;
}

std::vector<LockReply> LockTable::cancel(int32_t rank, uint64_t token, int error) {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::vector<LockReply> replies;
    for (auto &[file, locks] : m_files) {
        for (auto waiter = locks.waiters.begin(); waiter != locks.waiters.end(); ++waiter) {
            if (waiter->owner.rank == rank && waiter->token == token) {
                replies.push_back(replyTo(*waiter, error));
                locks.waiters.erase(waiter);
                const uint64_t id = file;
                forget(id);
                return replies;
            }
        }
    }
    return replies;
}

size_t LockTable::heldCount(uint64_t file) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    const auto entry = m_files.find(file);
    return entry == m_files.end() ? 0 : entry->second.held.size();
}

size_t LockTable::waiterCount(uint64_t file) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    const auto entry = m_files.find(file);
    return entry == m_files.end() ? 0 : entry->second.waiters.size();
}

} // namespace dagonfs
