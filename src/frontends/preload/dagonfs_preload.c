/* The interposer defines open() and friends itself; fortified inline
 * wrappers from the C library headers would clash with those definitions. */
#undef _FORTIFY_SOURCE
#define _GNU_SOURCE
#include "../native/Client.h"
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/*
 * Opt-in interposer. It only handles absolute paths below DAGONFS_PRELOAD_ROOT;
 * every other call goes to libc unchanged.
 *
 * A DAGonFS file is represented in the application by a real descriptor (a
 * duplicate of /dev/null), so its number can never collide with a descriptor
 * that libc hands out, and the slot table maps it to the volume handle and the
 * file offset that read(), write() and lseek() use.
 *
 * Linux resolves the real functions with dlsym(RTLD_NEXT). macOS uses dyld's
 * __interpose section, under which calls made from this image reach libc.
 */

#ifdef __APPLE__
#define REAL(name) name
#define WRAP(name) dagonfs_##name
#define INTERPOSE(name) \
    __attribute__((used)) static struct { const void *replacement; const void *original; } \
    interpose_##name __attribute__((section("__DATA,__interpose"))) = { (const void *)WRAP(name), (const void *)name }
#else
#define WRAP(name) name
#endif

#define SLOTS 1024
struct slot { int fd; int64_t handle; off_t offset; int flags; };
static struct slot slots[SLOTS];
static pthread_mutex_t slots_mutex = PTHREAD_MUTEX_INITIALIZER;

#ifndef __APPLE__
static void *next_symbol(const char *name) { return dlsym(RTLD_NEXT, name); }
#define DECLARE_REAL(ret, name, ...) \
    static ret (*real_##name)(__VA_ARGS__); \
    static ret (*resolve_##name(void))(__VA_ARGS__) { if (!real_##name) real_##name = next_symbol(#name); return real_##name; }
DECLARE_REAL(int, open, const char *, int, ...)
DECLARE_REAL(int, close, int)
DECLARE_REAL(ssize_t, read, int, void *, size_t)
DECLARE_REAL(ssize_t, write, int, const void *, size_t)
DECLARE_REAL(ssize_t, pread, int, void *, size_t, off_t)
DECLARE_REAL(ssize_t, pwrite, int, const void *, size_t, off_t)
DECLARE_REAL(off_t, lseek, int, off_t, int)
DECLARE_REAL(int, fsync, int)
DECLARE_REAL(int, unlink, const char *)
DECLARE_REAL(int, mkdir, const char *, mode_t)
DECLARE_REAL(int, rmdir, const char *)
DECLARE_REAL(int, rename, const char *, const char *)
#define CALL_REAL(name) (resolve_##name())
#else
#define CALL_REAL(name) REAL(name)
#endif

static int remote_path(const char *path, const char **inside) {
    const char *root = getenv("DAGONFS_PRELOAD_ROOT");
    size_t length;
    if (!root || !*root || !path || path[0] != '/') return 0;
    length = strlen(root);
    while (length > 1 && root[length - 1] == '/') --length;
    if (strncmp(path, root, length) != 0 || (path[length] && path[length] != '/')) return 0;
    *inside = path + length;
    if (!**inside) *inside = "/";
    return 1;
}

static struct slot *find_locked(int fd) {
    int i;
    if (fd < 0) return NULL;
    for (i = 0; i < SLOTS; ++i) if (slots[i].fd == fd) return &slots[i];
    return NULL;
}

/* Copies the slot out under the lock; returns 0 if fd is not a DAGonFS file. */
static int lookup(int fd, struct slot *copy) {
    struct slot *s;
    pthread_mutex_lock(&slots_mutex);
    s = find_locked(fd);
    if (s) *copy = *s;
    pthread_mutex_unlock(&slots_mutex);
    return s != NULL;
}

static void advance(int fd, off_t offset) {
    struct slot *s;
    pthread_mutex_lock(&slots_mutex);
    s = find_locked(fd);
    if (s) s->offset = offset;
    pthread_mutex_unlock(&slots_mutex);
}

static int attach(int64_t handle, int flags) {
    int i, fd = CALL_REAL(open)("/dev/null", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    pthread_mutex_lock(&slots_mutex);
    for (i = 0; i < SLOTS; ++i) {
        if (slots[i].fd == 0) {
            slots[i].fd = fd; slots[i].handle = handle; slots[i].offset = 0; slots[i].flags = flags;
            pthread_mutex_unlock(&slots_mutex);
            return fd;
        }
    }
    pthread_mutex_unlock(&slots_mutex);
    CALL_REAL(close)(fd);
    errno = EMFILE;
    return -1;
}

static int open_remote(const char *inside, int flags, mode_t mode) {
    int64_t handle = dagonfs_client_open(inside, flags, mode);
    int fd;
    if (handle < 0) return -1;
    fd = attach(handle, flags);
    if (fd < 0) { int saved = errno; dagonfs_client_close(handle); errno = saved; }
    return fd;
}

int WRAP(open)(const char *path, int flags, ...) {
    const char *inside;
    mode_t mode = 0;
    if (flags & O_CREAT) { va_list ap; va_start(ap, flags); mode = (mode_t) va_arg(ap, int); va_end(ap); }
    if (!remote_path(path, &inside)) return CALL_REAL(open)(path, flags, mode);
    return open_remote(inside, flags, mode);
}

int WRAP(close)(int fd) {
    struct slot s;
    if (!lookup(fd, &s)) return CALL_REAL(close)(fd);
    pthread_mutex_lock(&slots_mutex);
    { struct slot *p = find_locked(fd); if (p) p->fd = 0; }
    pthread_mutex_unlock(&slots_mutex);
    CALL_REAL(close)(fd);
    return dagonfs_client_close(s.handle);
}

ssize_t WRAP(pread)(int fd, void *buffer, size_t size, off_t offset) {
    struct slot s;
    if (!lookup(fd, &s)) return CALL_REAL(pread)(fd, buffer, size, offset);
    return dagonfs_client_pread(s.handle, buffer, size, offset);
}

ssize_t WRAP(pwrite)(int fd, const void *buffer, size_t size, off_t offset) {
    struct slot s;
    if (!lookup(fd, &s)) return CALL_REAL(pwrite)(fd, buffer, size, offset);
    return dagonfs_client_pwrite(s.handle, buffer, size, offset);
}

ssize_t WRAP(read)(int fd, void *buffer, size_t size) {
    struct slot s;
    ssize_t count;
    if (!lookup(fd, &s)) return CALL_REAL(read)(fd, buffer, size);
    count = dagonfs_client_pread(s.handle, buffer, size, s.offset);
    if (count > 0) advance(fd, s.offset + count);
    return count;
}

ssize_t WRAP(write)(int fd, const void *buffer, size_t size) {
    struct slot s;
    ssize_t count;
    off_t offset;
    if (!lookup(fd, &s)) return CALL_REAL(write)(fd, buffer, size);
    offset = s.offset;
    if (s.flags & O_APPEND) {
        /* The server appends at the end of file; the offset follows it. */
        struct stat st;
        count = dagonfs_client_pwrite(s.handle, buffer, size, 0);
        if (count < 0) return count;
        if (dagonfs_client_fstat(s.handle, &st) == 0) advance(fd, st.st_size);
        return count;
    }
    count = dagonfs_client_pwrite(s.handle, buffer, size, offset);
    if (count > 0) advance(fd, offset + count);
    return count;
}

off_t WRAP(lseek)(int fd, off_t offset, int whence) {
    struct slot s;
    off_t base;
    if (!lookup(fd, &s)) return CALL_REAL(lseek)(fd, offset, whence);
    if (whence == SEEK_SET) base = 0;
    else if (whence == SEEK_CUR) base = s.offset;
    else if (whence == SEEK_END) {
        struct stat st;
        if (dagonfs_client_fstat(s.handle, &st)) return -1;
        base = st.st_size;
    } else { errno = EINVAL; return -1; }
    if ((offset < 0 && base + offset < 0)) { errno = EINVAL; return -1; }
    advance(fd, base + offset);
    return base + offset;
}

int WRAP(fsync)(int fd) {
    struct slot s;
    if (!lookup(fd, &s)) return CALL_REAL(fsync)(fd);
    return dagonfs_client_fsync(s.handle);
}

int WRAP(unlink)(const char *path) {
    const char *inside;
    if (!remote_path(path, &inside)) return CALL_REAL(unlink)(path);
    return dagonfs_client_unlink(inside);
}

int WRAP(mkdir)(const char *path, mode_t mode) {
    const char *inside;
    if (!remote_path(path, &inside)) return CALL_REAL(mkdir)(path, mode);
    return dagonfs_client_mkdir(inside, mode);
}

int WRAP(rmdir)(const char *path) {
    const char *inside;
    if (!remote_path(path, &inside)) return CALL_REAL(rmdir)(path);
    return dagonfs_client_rmdir(inside);
}

int WRAP(rename)(const char *from, const char *to) {
    const char *a, *b;
    const int remoteFrom = remote_path(from, &a), remoteTo = remote_path(to, &b);
    if (!remoteFrom && !remoteTo) return CALL_REAL(rename)(from, to);
    if (remoteFrom != remoteTo) { errno = EXDEV; return -1; }
    return dagonfs_client_rename(a, b);
}

#ifdef __APPLE__
INTERPOSE(open);
INTERPOSE(close);
INTERPOSE(read);
INTERPOSE(write);
INTERPOSE(pread);
INTERPOSE(pwrite);
INTERPOSE(lseek);
INTERPOSE(fsync);
INTERPOSE(unlink);
INTERPOSE(mkdir);
INTERPOSE(rmdir);
INTERPOSE(rename);
#else
/* glibc programs built with _FILE_OFFSET_BITS=64 call the *64 names. */
int open64(const char *path, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) { va_list ap; va_start(ap, flags); mode = (mode_t) va_arg(ap, int); va_end(ap); }
    return open(path, flags, mode);
}
int openat(int dirfd, const char *path, int flags, ...) {
    static int (*real_openat)(int, const char *, int, ...);
    const char *inside;
    mode_t mode = 0;
    if (flags & O_CREAT) { va_list ap; va_start(ap, flags); mode = (mode_t) va_arg(ap, int); va_end(ap); }
    if (remote_path(path, &inside)) return open_remote(inside, flags, mode);
    if (!real_openat) real_openat = next_symbol("openat");
    return real_openat(dirfd, path, flags, mode);
}
int openat64(int dirfd, const char *path, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) { va_list ap; va_start(ap, flags); mode = (mode_t) va_arg(ap, int); va_end(ap); }
    return openat(dirfd, path, flags, mode);
}
ssize_t pread64(int fd, void *buffer, size_t size, off_t offset) { return pread(fd, buffer, size, offset); }
ssize_t pwrite64(int fd, const void *buffer, size_t size, off_t offset) { return pwrite(fd, buffer, size, offset); }
off_t lseek64(int fd, off_t offset, int whence) { return lseek(fd, offset, whence); }
#endif
