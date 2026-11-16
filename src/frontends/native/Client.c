#include "Client.h"
#include "Protocol.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

/* One connection per process, shared by its threads under the mutex. A child
 * created by fork() opens its own, so the two never interleave a stream. */
static pthread_mutex_t socket_mutex = PTHREAD_MUTEX_INITIALIZER;
static char socket_path[sizeof(((struct sockaddr_un *)0)->sun_path)];
static int connection = -1;
static pid_t connection_owner;

static int full_write(int fd, const void *data, size_t size) {
    const char *cursor = data;
    while (size) {
#ifdef MSG_NOSIGNAL
        ssize_t count = send(fd, cursor, size, MSG_NOSIGNAL);
#else
        ssize_t count = send(fd, cursor, size, 0);
#endif
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return -1;
        cursor += count;
        size -= (size_t) count;
    }
    return 0;
}

static int full_read(int fd, void *data, size_t size) {
    char *cursor = data;
    while (size) {
        ssize_t count = recv(fd, cursor, size, 0);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) { if (count == 0) errno = EPROTO; return -1; }
        cursor += count;
        size -= (size_t) count;
    }
    return 0;
}

int dagonfs_client_set_socket(const char *path) {
    if (!path || !*path || strlen(path) >= sizeof(socket_path)) { errno = ENAMETOOLONG; return -1; }
    pthread_mutex_lock(&socket_mutex);
    if (strcmp(socket_path, path) != 0) {
        strcpy(socket_path, path);
        if (connection >= 0 && connection_owner == getpid()) close(connection);
        connection = -1;
    }
    pthread_mutex_unlock(&socket_mutex);
    return 0;
}

const char *dagonfs_client_socket(void) {
    if (!socket_path[0]) {
        const char *environment = getenv("DAGONFS_SOCKET");
        if (environment) dagonfs_client_set_socket(environment);
    }
    return socket_path[0] ? socket_path : NULL;
}

static int connect_locked(const char *endpoint) {
    if (connection >= 0 && connection_owner == getpid()) return connection;
    /* After fork() the inherited descriptor belongs to the parent's stream. */
    if (connection >= 0) close(connection);
    connection = -1;
    struct sockaddr_un address = {.sun_family = AF_UNIX};
    if (strlen(endpoint) >= sizeof(address.sun_path)) { errno = ENAMETOOLONG; return -1; }
    strcpy(address.sun_path, endpoint);
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
#ifdef SO_NOSIGPIPE
    { int one = 1; setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)); }
#endif
    if (connect(fd, (struct sockaddr *)&address, sizeof(address))) { int saved = errno; close(fd); errno = saved; return -1; }
    connection = fd;
    connection_owner = getpid();
    return fd;
}

static void drop_locked(void) {
    if (connection >= 0) close(connection);
    connection = -1;
}

static int transact(struct dagonfs_native_request *request, const char *path, const char *second,
                    const void *out, struct dagonfs_native_reply *reply, void *in, size_t in_capacity) {
    const char *endpoint = dagonfs_client_socket();
    if (!endpoint) { errno = ENOTCONN; return -1; }
    request->magic = DAGONFS_NATIVE_MAGIC;
    request->version = DAGONFS_NATIVE_VERSION;
    request->path_length = path ? (uint32_t) strlen(path) : 0;
    request->second_path_length = second ? (uint32_t) strlen(second) : 0;
    if (request->path_length >= DAGONFS_NATIVE_MAX_PATH || request->second_path_length >= DAGONFS_NATIVE_MAX_PATH ||
        request->data_length > DAGONFS_NATIVE_MAX_IO) { errno = EINVAL; return -1; }
    pthread_mutex_lock(&socket_mutex);
    int fd = connect_locked(endpoint);
    if (fd < 0) { int saved = errno; pthread_mutex_unlock(&socket_mutex); errno = saved; return -1; }
    if (full_write(fd, request, sizeof(*request)) ||
        (request->path_length && full_write(fd, path, request->path_length)) ||
        (request->second_path_length && full_write(fd, second, request->second_path_length)) ||
        (request->data_length && full_write(fd, out, request->data_length)) ||
        full_read(fd, reply, sizeof(*reply))) {
        int saved = errno; drop_locked(); pthread_mutex_unlock(&socket_mutex); errno = saved; return -1;
    }
    if (reply->magic != DAGONFS_NATIVE_MAGIC || reply->version != DAGONFS_NATIVE_VERSION ||
        reply->data_length > DAGONFS_NATIVE_MAX_IO || (reply->data_length && (!in || reply->data_length > in_capacity))) {
        /* The stream position is unknown now: start over on a new connection. */
        drop_locked(); pthread_mutex_unlock(&socket_mutex); errno = EPROTO; return -1;
    }
    if (reply->data_length && full_read(fd, in, reply->data_length)) {
        int saved = errno; drop_locked(); pthread_mutex_unlock(&socket_mutex); errno = saved; return -1;
    }
    pthread_mutex_unlock(&socket_mutex);
    if (reply->error) { errno = reply->error; return -1; }
    return 0;
}

int64_t dagonfs_client_open(const char *path, int flags, mode_t mode) {
    struct dagonfs_native_request q = {.opcode=DAGONFS_NATIVE_OPEN, .flags=flags, .mode=(uint32_t)mode};
    struct dagonfs_native_reply r;
    return transact(&q, path, NULL, NULL, &r, NULL, 0) ? -1 : r.value;
}
int dagonfs_client_close(int64_t h) { struct dagonfs_native_request q={.opcode=DAGONFS_NATIVE_CLOSE,.handle=h}; struct dagonfs_native_reply r; return transact(&q,NULL,NULL,NULL,&r,NULL,0); }
ssize_t dagonfs_client_pread(int64_t h, void *b, size_t n, off_t o) {
    if (n > DAGONFS_NATIVE_MAX_IO) n = DAGONFS_NATIVE_MAX_IO;
    struct dagonfs_native_request q={.opcode=DAGONFS_NATIVE_READ,.data_length=(uint32_t)n,.handle=h,.offset=o}; struct dagonfs_native_reply r;
    q.data_length = 0; q.flags = (int32_t)n;
    return transact(&q,NULL,NULL,NULL,&r,b,n) ? -1 : (ssize_t)r.data_length;
}
ssize_t dagonfs_client_pwrite(int64_t h, const void *b, size_t n, off_t o) {
    if (n > DAGONFS_NATIVE_MAX_IO) n = DAGONFS_NATIVE_MAX_IO;
    struct dagonfs_native_request q={.opcode=DAGONFS_NATIVE_WRITE,.data_length=(uint32_t)n,.handle=h,.offset=o}; struct dagonfs_native_reply r;
    return transact(&q,NULL,NULL,b,&r,NULL,0) ? -1 : (ssize_t)r.value;
}
int dagonfs_client_fsync(int64_t h) { struct dagonfs_native_request q={.opcode=DAGONFS_NATIVE_FSYNC,.handle=h}; struct dagonfs_native_reply r; return transact(&q,NULL,NULL,NULL,&r,NULL,0); }
static void fill_stat(const struct dagonfs_native_reply *r, struct stat *s) {
    memset(s,0,sizeof(*s)); s->st_ino=(ino_t)r->inode; s->st_size=(off_t)r->size; s->st_mode=(mode_t)r->mode; s->st_nlink=(nlink_t)r->nlink; s->st_uid=(uid_t)r->uid; s->st_gid=(gid_t)r->gid;
#ifdef __APPLE__
    s->st_atimespec.tv_sec=r->atime_sec; s->st_mtimespec.tv_sec=r->mtime_sec; s->st_ctimespec.tv_sec=r->ctime_sec;
#else
    s->st_atim.tv_sec=r->atime_sec; s->st_mtim.tv_sec=r->mtime_sec; s->st_ctim.tv_sec=r->ctime_sec;
#endif
}
int dagonfs_client_stat(const char *p, struct stat *s) {
    struct dagonfs_native_request q={.opcode=DAGONFS_NATIVE_STAT}; struct dagonfs_native_reply r;
    if (transact(&q,p,NULL,NULL,&r,NULL,0)) return -1;
    fill_stat(&r,s); return 0;
}
int dagonfs_client_fstat(int64_t h, struct stat *s) {
    struct dagonfs_native_request q={.opcode=DAGONFS_NATIVE_FSTAT,.handle=h}; struct dagonfs_native_reply r;
    if (transact(&q,NULL,NULL,NULL,&r,NULL,0)) return -1;
    fill_stat(&r,s); return 0;
}
static int path_call(uint16_t op,const char *p,mode_t m){struct dagonfs_native_request q={.opcode=op,.mode=(uint32_t)m};struct dagonfs_native_reply r;return transact(&q,p,NULL,NULL,&r,NULL,0);}
int dagonfs_client_mkdir(const char *p, mode_t m){return path_call(DAGONFS_NATIVE_MKDIR,p,m);}
int dagonfs_client_unlink(const char *p){return path_call(DAGONFS_NATIVE_UNLINK,p,0);}
int dagonfs_client_rmdir(const char *p){return path_call(DAGONFS_NATIVE_RMDIR,p,0);}
int dagonfs_client_rename(const char *a,const char *b){struct dagonfs_native_request q={.opcode=DAGONFS_NATIVE_RENAME};struct dagonfs_native_reply r;return transact(&q,a,b,NULL,&r,NULL,0);}
ssize_t dagonfs_client_list(const char *p,char *b,size_t n){
    struct dagonfs_native_request q={.opcode=DAGONFS_NATIVE_LIST,.flags=(int32_t)(n>INT32_MAX?INT32_MAX:n)};struct dagonfs_native_reply r;
    if (transact(&q,p,NULL,NULL,&r,b,n)) return errno==ERANGE ? (ssize_t)r.value : -1;
    return (ssize_t)r.data_length;
}
int dagonfs_client_shutdown(void){
    struct dagonfs_native_request q={.opcode=DAGONFS_NATIVE_SHUTDOWN};struct dagonfs_native_reply r;
    int status=transact(&q,NULL,NULL,NULL,&r,NULL,0);
    pthread_mutex_lock(&socket_mutex); drop_locked(); pthread_mutex_unlock(&socket_mutex);
    return status;
}
