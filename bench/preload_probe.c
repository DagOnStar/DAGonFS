/* Positional-I/O probe run under the DAGonFS preload interposer:
 *     preload_probe PATH [OPS [SIZE]]
 * Prints {"pwrite": ops/s, "pread": ops/s}. */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec / 1e9; }

int main(int argc, char **argv) {
    if (argc < 2 || argc > 4) { fprintf(stderr, "usage: %s PATH [OPS [SIZE]]\n", argv[0]); return 2; }
    const long ops = argc > 2 ? atol(argv[2]) : 100;
    const size_t size = argc > 3 ? (size_t) atol(argv[3]) : 4096;
    if (ops <= 0 || size == 0) return 2;
    char *data = malloc(size);
    if (!data) return 1;
    memset(data, 'x', size);
    int fd = open(argv[1], O_CREAT | O_RDWR | O_TRUNC, 0600);
    if (fd < 0) { perror("open"); return 1; }
    double start = now();
    for (long i = 0; i < ops; ++i)
        if (pwrite(fd, data, size, (off_t) i * (off_t) size) != (ssize_t) size) { perror("pwrite"); return 1; }
    double writes = ops / (now() - start);
    start = now();
    for (long i = 0; i < ops; ++i)
        if (pread(fd, data, size, (off_t) i * (off_t) size) != (ssize_t) size) { perror("pread"); return 1; }
    double reads = ops / (now() - start);
    if (close(fd)) { perror("close"); return 1; }
    free(data);
    printf("{\"pwrite\":%.3f,\"pread\":%.3f}\n", writes, reads);
    return 0;
}
