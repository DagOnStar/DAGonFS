//
// DAGonFS volume options: deployment model, mount root and run-time flags.
//
// This header has no MPI or FUSE dependency, so the launcher and other tools
// can share the parsing of DAGonFS.ini and of the command line.
//

#ifndef DAGONFS_OPTIONS_HPP
#define DAGONFS_OPTIONS_HPP

#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace dagonfs {

/** How a volume is distributed over the MPI ranks. */
enum class Model {
    /** Rank 0 mounts the volume at the root; ranks 1..P-1 only store blocks. */
    ClientServer,
    /** Every rank r mounts the volume at <root>/<r>; namespace changes are replicated. */
    PeerToPeer,
};

/** How applications reach a volume. */
enum class Frontend {
    /** Kernel VFS mount through libfuse3. */
    Fuse,
    /** Unix-domain socket protocol used by the C and MPI-aware clients. */
    Native,
    /** Native protocol plus an LD_PRELOAD POSIX interposer and virtual path prefix. */
    Preload,
};

std::optional<Frontend> parseFrontend(std::string_view text);
const char *frontendName(Frontend frontend);

/** Parse "client-server" / "cs" or "peer-to-peer" / "p2p" (case-sensitive). */
std::optional<Model> parseModel(std::string_view text);
/** Canonical name: "client-server" or "peer-to-peer". */
const char *modelName(Model model);

/** Everything needed to create (mount and serve) a DAGonFS volume. */
struct VolumeOptions {
    Model model = Model::ClientServer;
    Frontend frontend = Frontend::Fuse;
    /** FUSE mount root, or the native/preload frontend's Unix socket path. */
    std::string root;
    /** Trace every FUSE request (FUSE "-d"); FUSE always runs in the foreground. */
    bool debug = false;
    /** Enable verbose log4cplus output. */
    bool logging = false;

    /** True if @p rank exposes a mount point. */
    bool mounts(int rank) const;
    /** Mount point of @p rank, or "" if that rank does not mount the volume. */
    std::string mountPoint(int rank) const;
};

/** Key/value pairs of a DAGonFS.ini file. */
using IniValues = std::map<std::string, std::string>;

/** Read `key = value` lines; '#'/';' comments and [section] headers are ignored. */
bool readIni(const std::string &path, IniValues &values, std::string &error);

/**
 * Apply the volume keys of an ini file: dagonfs_model, dagonfs_root_dir,
 * fuse_mode (f|d) and enable_logging (true|false|1|0). Absent keys keep the
 * current value. Launcher-only keys are ignored.
 */
bool applyIni(const IniValues &values, VolumeOptions &options, std::string &error);

enum class ParseStatus { Ok, Help, Error };

/**
 * Parse `dagonfs [--model M] [--config FILE] [-f|-d] [--log] <root> [0|1]`.
 *
 * Precedence, lowest first: built-in defaults; the program name
 * (DAGonFS_CS.exe / DAGonFS_P2P.exe select their model); the ini file given
 * with --config; explicit command-line flags.
 */
ParseStatus parseCommandLine(int argc, const char *const argv[], VolumeOptions &options, std::string &error);

/** Multi-line usage text for @p program. */
std::string usage(const std::string &program);

} // namespace dagonfs

#endif // DAGONFS_OPTIONS_HPP
