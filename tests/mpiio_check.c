/*
 * Parallel MPI-IO on a mounted DAGonFS volume.
 *
 *   mpirun -np N dagonfs_mpiio_check run    <directory> [blocks] [block-bytes]
 *   mpirun -np M dagonfs_mpiio_check verify <directory> <writers> [blocks] [block-bytes]
 *   mpirun -np N dagonfs_mpiio_check peers  <directory> [<directory>...]
 *
 * "run" is started as an MPI job of its own, separate from the DAGonFS job;
 * all its ranks open files in <directory>, a directory of a DAGonFS mount
 * point. It exercises, on shared files:
 *
 *   1. independent contiguous writes and reads at explicit offsets
 *      (MPI_File_write_at / read_at), each rank reading another rank's data;
 *   2. collective strided I/O through a file view (MPI_File_write_all /
 *      read_all with a vector filetype);
 *   3. concurrent independent non-contiguous writes that leave holes, which
 *      ROMIO serves by data sieving: read-modify-write under fcntl locks, so
 *      a piece is lost unless the locks and read-after-write are coherent;
 *   4. atomic mode (MPI_File_set_atomicity), which also takes byte-range locks;
 *   5. the shared file pointer (MPI_File_write_ordered);
 *   6. MPI_File_set_size, MPI_File_get_size, MPI_File_sync, MPI_File_delete.
 *
 * Every byte read is compared with the expected pattern. "run" keeps
 * <directory>/independent.dat, which "verify" checks from another mount point
 * (in peer-to-peer mode, another peer) after the writers closed it.
 *
 * "peers" checks the sync-barrier-sync rule with one file per process: rank r
 * works in the r-th directory (modulo their number), so that, given the same
 * directory as seen through two peers' mount points, neighbouring ranks use
 * different peers. Each rank opens its own file and, before any data exist,
 * its neighbour's; it writes, calls MPI_File_sync, meets the others at a
 * barrier, calls MPI_File_sync on the neighbour's file and reads it. It then
 * overwrites its file and the neighbour reads the new content through the
 * descriptor it kept open.
 * Exit status 0 means every check passed; a failure aborts the job.
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int rank, size;

static void fail(const char *what, int code) {
    char message[MPI_MAX_ERROR_STRING] = "";
    int length = 0;
    if (code != MPI_SUCCESS) MPI_Error_string(code, message, &length);
    fprintf(stderr, "mpiio rank %d: FAILED: %s %s\n", rank, what, message);
    fflush(stderr);
    MPI_Abort(MPI_COMM_WORLD, 1);
}

#define CHECK(call)                                   \
    do {                                              \
        int code_ = (call);                           \
        if (code_ != MPI_SUCCESS) fail(#call, code_); \
    } while (0)

/* Byte k of block j written by rank r. */
static unsigned char pattern(int r, long j, long k) {
    return (unsigned char) ((r * 131 + j * 31 + k + 1) & 0xff);
}

static void fill(unsigned char *buffer, int r, long j, long bytes) {
    for (long k = 0; k < bytes; k++) buffer[k] = pattern(r, j, k);
}

static void expect(const unsigned char *buffer, int r, long j, long bytes, const char *scenario) {
    for (long k = 0; k < bytes; k++) {
        if (buffer[k] != pattern(r, j, k)) {
            char what[256];
            snprintf(what, sizeof what, "%s: block %ld of rank %d differs at byte %ld (%u, expected %u)",
                     scenario, j, r, k, buffer[k], pattern(r, j, k));
            fail(what, MPI_SUCCESS);
        }
    }
}

static void path(char *out, size_t length, const char *directory, const char *name) {
    if ((size_t) snprintf(out, length, "%s/%s", directory, name) >= length) fail("path too long", MPI_SUCCESS);
}

static void expect_size(MPI_File file, MPI_Offset expected, const char *scenario) {
    MPI_Offset actual = 0;
    CHECK(MPI_File_get_size(file, &actual));
    if (actual != expected) {
        char what[256];
        snprintf(what, sizeof what, "%s: file size %lld, expected %lld", scenario, (long long) actual,
                 (long long) expected);
        fail(what, MPI_SUCCESS);
    }
}

static void done(const char *scenario) {
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) printf("mpiio %s: ok\n", scenario), fflush(stdout);
}

/* 1. Block j of rank r is at ((j * size) + r) * bs: ranks interleave block by block. */
static void independent(const char *directory, long blocks, long bs, unsigned char *buffer) {
    char name[4096];
    path(name, sizeof name, directory, "independent.dat");
    MPI_File file;
    CHECK(MPI_File_open(MPI_COMM_WORLD, name, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &file));
    for (long j = 0; j < blocks; j++) {
        fill(buffer, rank, j, bs);
        CHECK(MPI_File_write_at(file, (j * size + rank) * bs, buffer, (int) bs, MPI_BYTE, MPI_STATUS_IGNORE));
    }
    CHECK(MPI_File_sync(file));
    MPI_Barrier(MPI_COMM_WORLD);
    CHECK(MPI_File_sync(file));
    const int other = (rank + 1) % size;
    for (long j = 0; j < blocks; j++) {
        CHECK(MPI_File_read_at(file, (j * size + other) * bs, buffer, (int) bs, MPI_BYTE, MPI_STATUS_IGNORE));
        expect(buffer, other, j, bs, "independent");
    }
    expect_size(file, (MPI_Offset) blocks * size * bs, "independent");
    CHECK(MPI_File_close(&file));
    done("independent write_at/read_at");
}

/* 2. The same layout through a vector file view, written and read collectively. */
static void collective(const char *directory, long blocks, long bs, unsigned char *buffer) {
    char name[4096];
    path(name, sizeof name, directory, "collective.dat");
    MPI_Datatype view;
    CHECK(MPI_Type_vector((int) blocks, (int) bs, (int) (bs * size), MPI_BYTE, &view));
    CHECK(MPI_Type_commit(&view));
    MPI_File file;
    CHECK(MPI_File_open(MPI_COMM_WORLD, name, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &file));
    CHECK(MPI_File_set_view(file, (MPI_Offset) rank * bs, MPI_BYTE, view, "native", MPI_INFO_NULL));
    for (long j = 0; j < blocks; j++) fill(buffer + j * bs, rank, j, bs);
    CHECK(MPI_File_write_all(file, buffer, (int) (blocks * bs), MPI_BYTE, MPI_STATUS_IGNORE));
    CHECK(MPI_File_sync(file));
    MPI_Barrier(MPI_COMM_WORLD);
    CHECK(MPI_File_sync(file));
    const int other = (rank + size - 1) % size;
    CHECK(MPI_File_set_view(file, (MPI_Offset) other * bs, MPI_BYTE, view, "native", MPI_INFO_NULL));
    memset(buffer, 0, (size_t) (blocks * bs));
    CHECK(MPI_File_read_all(file, buffer, (int) (blocks * bs), MPI_BYTE, MPI_STATUS_IGNORE));
    for (long j = 0; j < blocks; j++) expect(buffer + j * bs, other, j, bs, "collective");
    expect_size(file, (MPI_Offset) blocks * size * bs, "collective");
    CHECK(MPI_File_close(&file));
    CHECK(MPI_Type_free(&view));
    if (rank == 0) CHECK(MPI_File_delete(name, MPI_INFO_NULL));
    done("collective write_all/read_all with a file view");
}

/*
 * 3. Rank r owns pieces of `piece` bytes at (j * size + r) * slot, slot > piece:
 * the gaps between the pieces stay holes. Independent non-contiguous writes
 * make ROMIO read the covering range, merge and write it back, under a lock,
 * while the other ranks do the same on interleaved ranges.
 */
static void sieving(const char *directory, long blocks, unsigned char *buffer) {
    const long piece = 1000, slot = 1024;
    char name[4096];
    path(name, sizeof name, directory, "sieving.dat");
    MPI_Datatype vector;
    CHECK(MPI_Type_vector((int) blocks, (int) piece, (int) (slot * size), MPI_BYTE, &vector));
    CHECK(MPI_Type_commit(&vector));
    MPI_File file;
    CHECK(MPI_File_open(MPI_COMM_WORLD, name, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &file));
    CHECK(MPI_File_set_view(file, (MPI_Offset) rank * slot, MPI_BYTE, vector, "native", MPI_INFO_NULL));
    for (long j = 0; j < blocks; j++) fill(buffer + j * piece, rank, j, piece);
    CHECK(MPI_File_write(file, buffer, (int) (blocks * piece), MPI_BYTE, MPI_STATUS_IGNORE));
    CHECK(MPI_File_sync(file));
    MPI_Barrier(MPI_COMM_WORLD);
    CHECK(MPI_File_sync(file));
    CHECK(MPI_File_set_view(file, 0, MPI_BYTE, MPI_BYTE, "native", MPI_INFO_NULL));
    /*
     * Every rank checks every piece: none may be lost by another rank's
     * read-modify-write. The holes are not checked: MPI leaves bytes that were
     * never written undefined, and ROMIO's data sieving writes back whatever
     * its read buffer held past the end of file (also on local file systems).
     */
    const long total = (blocks - 1) * slot * size + (size - 1) * slot + piece;
    unsigned char *whole = calloc((size_t) total, 1);
    if (whole == NULL) fail("calloc", MPI_SUCCESS);
    CHECK(MPI_File_read_at(file, 0, whole, (int) total, MPI_BYTE, MPI_STATUS_IGNORE));
    for (long j = 0; j < blocks; j++) {
        for (int r = 0; r < size; r++) {
            expect(whole + (j * size + r) * slot, r, j, piece, "sieving");
        }
    }
    free(whole);
    expect_size(file, total, "sieving");
    CHECK(MPI_File_close(&file));
    CHECK(MPI_Type_free(&vector));
    if (rank == 0) CHECK(MPI_File_delete(name, MPI_INFO_NULL));
    done("concurrent non-contiguous independent writes (data sieving)");
}

/* 4. Atomic mode, then 5. the shared file pointer, then 6. resizing. */
static void atomic_ordered_resize(const char *directory, long bs, unsigned char *buffer) {
    char name[4096];
    path(name, sizeof name, directory, "atomic.dat");
    MPI_File file;
    CHECK(MPI_File_open(MPI_COMM_WORLD, name, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &file));
    CHECK(MPI_File_set_atomicity(file, 1));
    int atomic = 0;
    CHECK(MPI_File_get_atomicity(file, &atomic));
    if (!atomic) fail("atomic mode was not enabled", MPI_SUCCESS);
    fill(buffer, rank, 0, bs);
    CHECK(MPI_File_write_at(file, (MPI_Offset) rank * bs, buffer, (int) bs, MPI_BYTE, MPI_STATUS_IGNORE));
    MPI_Barrier(MPI_COMM_WORLD);
    const int other = (rank + 1) % size;
    CHECK(MPI_File_read_at(file, (MPI_Offset) other * bs, buffer, (int) bs, MPI_BYTE, MPI_STATUS_IGNORE));
    expect(buffer, other, 0, bs, "atomic");
    CHECK(MPI_File_set_atomicity(file, 0));
    CHECK(MPI_File_close(&file));
    if (rank == 0) CHECK(MPI_File_delete(name, MPI_INFO_NULL));
    done("atomic mode");

    path(name, sizeof name, directory, "ordered.dat");
    CHECK(MPI_File_open(MPI_COMM_WORLD, name, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &file));
    const long line = 64;
    fill(buffer, rank, 7, line);
    CHECK(MPI_File_write_ordered(file, buffer, (int) line, MPI_BYTE, MPI_STATUS_IGNORE));
    CHECK(MPI_File_sync(file));
    MPI_Barrier(MPI_COMM_WORLD);
    CHECK(MPI_File_sync(file));
    for (int r = 0; r < size; r++) {
        CHECK(MPI_File_read_at(file, (MPI_Offset) r * line, buffer, (int) line, MPI_BYTE, MPI_STATUS_IGNORE));
        expect(buffer, r, 7, line, "ordered");
    }
    expect_size(file, (MPI_Offset) size * line, "ordered");

    /*
     * Collective resizing: shrink to one line, then grow with a zero tail. The
     * barriers are needed: an implementation may truncate on one rank as soon
     * as it enters MPI_File_set_size, while others still check the size.
     */
    MPI_Barrier(MPI_COMM_WORLD);
    CHECK(MPI_File_set_size(file, line));
    expect_size(file, line, "set_size shrink");
    MPI_Barrier(MPI_COMM_WORLD);
    CHECK(MPI_File_set_size(file, 3 * line));
    expect_size(file, 3 * line, "set_size grow");
    CHECK(MPI_File_read_at(file, 0, buffer, (int) (3 * line), MPI_BYTE, MPI_STATUS_IGNORE));
    expect(buffer, 0, 7, line, "set_size");
    for (long k = line; k < 3 * line; k++) {
        if (buffer[k] != 0) fail("set_size: the grown tail is not zero", MPI_SUCCESS);
    }
    CHECK(MPI_File_close(&file));
    if (rank == 0) CHECK(MPI_File_delete(name, MPI_INFO_NULL));
    done("shared file pointer (write_ordered) and set_size");
}

/* Reads independent.dat, written by `writers` ranks, from another mount point. */
static int verify(const char *directory, int writers, long blocks, long bs, unsigned char *buffer) {
    char name[4096];
    path(name, sizeof name, directory, "independent.dat");
    MPI_File file;
    CHECK(MPI_File_open(MPI_COMM_WORLD, name, MPI_MODE_RDONLY, MPI_INFO_NULL, &file));
    expect_size(file, (MPI_Offset) blocks * writers * bs, "verify");
    for (long j = rank; j < blocks; j += size) {
        for (int r = 0; r < writers; r++) {
            CHECK(MPI_File_read_at(file, (j * writers + r) * bs, buffer, (int) bs, MPI_BYTE, MPI_STATUS_IGNORE));
            expect(buffer, r, j, bs, "verify");
        }
    }
    CHECK(MPI_File_close(&file));
    done("verify from another mount point");
    return 0;
}

/* Opens `name` on MPI_COMM_SELF, waiting up to 10 s for a name created on another peer to appear. */
static void open_self(const char *name, int mode, MPI_File *file) {
    const struct timespec pause = {0, 100 * 1000 * 1000};
    int code = MPI_SUCCESS;
    for (int attempt = 0; attempt < 100; attempt++) {
        code = MPI_File_open(MPI_COMM_SELF, name, mode, MPI_INFO_NULL, file);
        if (code == MPI_SUCCESS) return;
        nanosleep(&pause, NULL);
    }
    fail(name, code);
}

static void peers(int directories, char **directory, long blocks, long bs, unsigned char *buffer) {
    const int other = (rank + 1) % size;
    char mine[4096], theirs[4096], leaf[64];
    snprintf(leaf, sizeof leaf, "peer%d.dat", rank);
    path(mine, sizeof mine, directory[rank % directories], leaf);
    snprintf(leaf, sizeof leaf, "peer%d.dat", other);
    path(theirs, sizeof theirs, directory[rank % directories], leaf);

    MPI_File own, neighbour;
    open_self(mine, MPI_MODE_CREATE | MPI_MODE_RDWR, &own);
    MPI_Barrier(MPI_COMM_WORLD);
    /* Opened before the neighbour writes: the content must not stay the empty snapshot. */
    open_self(theirs, MPI_MODE_RDWR, &neighbour);
    MPI_Barrier(MPI_COMM_WORLD);
    for (int round = 0; round < 2; round++) {
        /* Round 1 overwrites every block with different content (block numbers shifted). */
        for (long j = 0; j < blocks; j++) {
            fill(buffer, rank, j + round * blocks, bs);
            CHECK(MPI_File_write_at(own, j * bs, buffer, (int) bs, MPI_BYTE, MPI_STATUS_IGNORE));
        }
        CHECK(MPI_File_sync(own));
        MPI_Barrier(MPI_COMM_WORLD);
        CHECK(MPI_File_sync(neighbour));
        expect_size(neighbour, (MPI_Offset) blocks * bs, "peers");
        for (long j = 0; j < blocks; j++) {
            CHECK(MPI_File_read_at(neighbour, j * bs, buffer, (int) bs, MPI_BYTE, MPI_STATUS_IGNORE));
            expect(buffer, other, j + round * blocks, bs, round == 0 ? "peers, first write" : "peers, overwrite");
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
    CHECK(MPI_File_close(&neighbour));
    CHECK(MPI_File_close(&own));
    MPI_Barrier(MPI_COMM_WORLD);
    CHECK(MPI_File_delete(mine, MPI_INFO_NULL));
    char what[128];
    snprintf(what, sizeof what, "file per process through %d mount point(s), sync-barrier-sync", directories);
    done(what);
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    const int isVerify = argc >= 4 && strcmp(argv[1], "verify") == 0;
    const int isPeers = argc >= 3 && strcmp(argv[1], "peers") == 0;
    if (!(argc >= 3 && strcmp(argv[1], "run") == 0) && !isVerify && !isPeers) {
        if (rank == 0) {
            fprintf(stderr, "usage: %s run <directory> [blocks] [block-bytes]\n"
                            "       %s verify <directory> <writers> [blocks] [block-bytes]\n"
                            "       %s peers <directory> [<directory>...]\n", argv[0], argv[0], argv[0]);
        }
        MPI_Finalize();
        return 2;
    }
    const int first = isPeers ? argc : isVerify ? 4 : 3;
    const long blocks = argc > first ? atol(argv[first]) : 16;
    const long bs = argc > first + 1 ? atol(argv[first + 1]) : 300000; /* not a multiple of the 128 KiB block */
    if (blocks < 1 || bs < 1000 || blocks * bs > (1L << 30)) fail("invalid blocks or block-bytes", MPI_SUCCESS);
    unsigned char *buffer = malloc((size_t) (blocks * bs));
    if (buffer == NULL) fail("malloc", MPI_SUCCESS);

    if (isPeers) {
        peers(argc - 2, argv + 2, blocks, bs, buffer);
    } else if (isVerify) {
        const int writers = atoi(argv[3]);
        if (writers < 1) fail("invalid writers", MPI_SUCCESS);
        verify(argv[2], writers, blocks, bs, buffer);
    } else {
        independent(argv[2], blocks, bs, buffer);
        collective(argv[2], blocks, bs, buffer);
        sieving(argv[2], blocks, buffer);
        atomic_ordered_resize(argv[2], bs, buffer);
    }
    free(buffer);
    MPI_Finalize();
    return 0;
}
