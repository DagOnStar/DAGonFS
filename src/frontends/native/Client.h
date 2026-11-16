#ifndef DAGONFS_NATIVE_CLIENT_H
#define DAGONFS_NATIVE_CLIENT_H

#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Set the Unix socket used by subsequent calls. The value is copied. */
int dagonfs_client_set_socket(const char *path);
const char *dagonfs_client_socket(void);

int64_t dagonfs_client_open(const char *path, int flags, mode_t mode);
int dagonfs_client_close(int64_t handle);
ssize_t dagonfs_client_pread(int64_t handle, void *buffer, size_t size, off_t offset);
ssize_t dagonfs_client_pwrite(int64_t handle, const void *buffer, size_t size, off_t offset);
int dagonfs_client_fsync(int64_t handle);
int dagonfs_client_stat(const char *path, struct stat *result);
int dagonfs_client_fstat(int64_t handle, struct stat *result);
int dagonfs_client_mkdir(const char *path, mode_t mode);
int dagonfs_client_unlink(const char *path);
int dagonfs_client_rmdir(const char *path);
int dagonfs_client_rename(const char *old_path, const char *new_path);
/** Writes newline-separated entry names (not NUL-terminated) and returns their
 * length. If the buffer is too small, returns the required length with errno
 * set to ERANGE and leaves the buffer unchanged. */
ssize_t dagonfs_client_list(const char *path, char *buffer, size_t size);
int dagonfs_client_shutdown(void);

#ifdef __cplusplus
}
#endif
#endif
