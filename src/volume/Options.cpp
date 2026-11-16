//
// DAGonFS volume options (see Options.hpp).
//

#include "Options.hpp"

#include <fstream>
#include <vector>

namespace dagonfs {

namespace {

std::string trim(const std::string &value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::string baseName(const std::string &path) {
    const auto slash = path.find_last_of('/');
    return slash == std::string::npos ? path : path.substr(slash + 1);
}

std::optional<bool> parseBool(const std::string &text) {
    if (text == "true" || text == "1" || text == "yes" || text == "on") return true;
    if (text == "false" || text == "0" || text == "no" || text == "off" || text.empty()) return false;
    return std::nullopt;
}

} // namespace

std::optional<Model> parseModel(std::string_view text) {
    if (text == "client-server" || text == "cs") return Model::ClientServer;
    if (text == "peer-to-peer" || text == "p2p") return Model::PeerToPeer;
    return std::nullopt;
}

const char *modelName(Model model) {
    return model == Model::PeerToPeer ? "peer-to-peer" : "client-server";
}

std::optional<Frontend> parseFrontend(std::string_view text) {
    if (text == "fuse" || text == "fuse3") return Frontend::Fuse;
    if (text == "native" || text == "mpi-io" || text == "mpiio") return Frontend::Native;
    if (text == "preload" || text == "interpose" || text == "ld-preload") return Frontend::Preload;
    return std::nullopt;
}

const char *frontendName(Frontend frontend) {
    if (frontend == Frontend::Native) return "native";
    if (frontend == Frontend::Preload) return "preload";
    return "fuse3";
}

bool VolumeOptions::mounts(int rank) const {
    if (frontend != Frontend::Fuse) return rank == 0;
    return model == Model::PeerToPeer || rank == 0;
}

std::string VolumeOptions::mountPoint(int rank) const {
    if (!mounts(rank)) return "";
    if (model == Model::ClientServer) return root;
    std::string path = root;
    while (path.size() > 1 && path.back() == '/') path.pop_back();
    return path + "/" + std::to_string(rank);
}

bool readIni(const std::string &path, IniValues &values, std::string &error) {
    std::ifstream file(path);
    if (!file) {
        error = "unable to open configuration file '" + path + "'";
        return false;
    }
    std::string line;
    unsigned lineNumber = 0;
    while (std::getline(file, line)) {
        ++lineNumber;
        line = trim(line);
        if (line.empty() || line[0] == '#' || line[0] == ';' || line[0] == '[') continue;
        const size_t pos = line.find('=');
        if (pos == std::string::npos) {
            error = path + ":" + std::to_string(lineNumber) + ": expected 'key = value'";
            return false;
        }
        // Trailing comments are allowed after a value.
        std::string value = line.substr(pos + 1);
        const size_t comment = value.find(" #");
        if (comment != std::string::npos) value = value.substr(0, comment);
        values[trim(line.substr(0, pos))] = trim(value);
    }
    return true;
}

bool applyIni(const IniValues &values, VolumeOptions &options, std::string &error) {
    if (auto entry = values.find("dagonfs_model"); entry != values.end()) {
        const auto model = parseModel(entry->second);
        if (!model) {
            error = "dagonfs_model must be 'client-server' or 'peer-to-peer', not '" + entry->second + "'";
            return false;
        }
        options.model = *model;
    }
    if (auto entry = values.find("dagonfs_root_dir"); entry != values.end() && !entry->second.empty()) {
        options.root = entry->second;
    }
    if (auto entry = values.find("dagonfs_frontend"); entry != values.end()) {
        const auto frontend = parseFrontend(entry->second);
        if (!frontend) {
            error = "dagonfs_frontend must be 'fuse3', 'native' or 'preload', not '" + entry->second + "'";
            return false;
        }
        options.frontend = *frontend;
    }
    if (auto entry = values.find("fuse_mode"); entry != values.end()) {
        if (entry->second != "f" && entry->second != "d") {
            error = "fuse_mode must be 'f' (foreground) or 'd' (debug)";
            return false;
        }
        options.debug = entry->second == "d";
    }
    if (auto entry = values.find("enable_logging"); entry != values.end()) {
        const auto enabled = parseBool(entry->second);
        if (!enabled) {
            error = "enable_logging must be 'true' or 'false'";
            return false;
        }
        options.logging = *enabled;
    }
    return true;
}

ParseStatus parseCommandLine(int argc, const char *const argv[], VolumeOptions &options, std::string &error) {
    const std::vector<std::string> args(argv + 1, argv + argc);

    // 1. The program name selects a model (backward compatibility).
    const std::string program = argc > 0 ? baseName(argv[0]) : "dagonfs";
    if (program.find("_P2P") != std::string::npos) options.model = Model::PeerToPeer;
    if (program.find("_CS") != std::string::npos) options.model = Model::ClientServer;

    // 2. A configuration file, wherever it appears, is applied before the flags.
    for (size_t i = 0; i < args.size(); ++i) {
        std::string file;
        if ((args[i] == "--config" || args[i] == "-c") && i + 1 < args.size()) file = args[i + 1];
        else if (args[i].rfind("--config=", 0) == 0) file = args[i].substr(9);
        else continue;
        IniValues values;
        if (!readIni(file, values, error) || !applyIni(values, options, error)) return ParseStatus::Error;
    }

    // 3. Explicit flags.
    std::vector<std::string> positional;
    for (size_t i = 0; i < args.size(); ++i) {
        const std::string &arg = args[i];
        if (arg == "-h" || arg == "--help") return ParseStatus::Help;
        if (arg == "--config" || arg == "-c") { ++i; continue; }
        if (arg.rfind("--config=", 0) == 0) continue;
        if (arg == "-f") { options.debug = false; continue; }
        if (arg == "-d") { options.debug = true; continue; }
        if (arg == "--log") { options.logging = true; continue; }
        std::string frontendText;
        bool isFrontend = false;
        if (arg == "--frontend") {
            if (i + 1 >= args.size()) { error = arg + " requires a value"; return ParseStatus::Error; }
            frontendText = args[++i];
            isFrontend = true;
        } else if (arg.rfind("--frontend=", 0) == 0) {
            frontendText = arg.substr(11);
            isFrontend = true;
        }
        if (isFrontend) {
            const auto frontend = parseFrontend(frontendText);
            if (!frontend) { error = "unknown frontend '" + frontendText + "'"; return ParseStatus::Error; }
            options.frontend = *frontend;
            continue;
        }
        std::string modelText;
        bool isModel = false;
        if ((arg == "--model" || arg == "-m")) {
            if (i + 1 >= args.size()) { error = arg + " requires a value"; return ParseStatus::Error; }
            modelText = args[++i];
            isModel = true;
        } else if (arg.rfind("--model=", 0) == 0) {
            modelText = arg.substr(8);
            isModel = true;
        }
        if (isModel) {
            const auto model = parseModel(modelText);
            if (!model) { error = "unknown model '" + modelText + "'"; return ParseStatus::Error; }
            options.model = *model;
            continue;
        }
        if (arg.size() > 1 && arg[0] == '-') { error = "unknown option '" + arg + "'"; return ParseStatus::Error; }
        positional.push_back(arg);
    }

    // Legacy trailing log flag: "<root> 0|1".
    if (positional.size() == 2 && (positional[1] == "0" || positional[1] == "1")) {
        options.logging = positional[1] == "1";
        positional.pop_back();
    }
    if (positional.size() > 1) { error = "more than one mount root given"; return ParseStatus::Error; }
    if (positional.size() == 1) options.root = positional[0];
    if (options.root.empty()) { error = "no mount root given"; return ParseStatus::Error; }
    return ParseStatus::Ok;
}

std::string usage(const std::string &program) {
    return "Usage: mpirun -np <P> [--hostfile <file>] " + program +
           " [--model client-server|peer-to-peer] [--frontend fuse3|native|preload] [--config DAGonFS.ini] [-f|-d] [--log] <root> [0|1]\n"
           "\n"
           "  -m, --model M    client-server: rank 0 mounts <root>, other ranks store blocks\n"
           "                   peer-to-peer:  rank r mounts <root>/<r>, namespace is replicated\n"
           "      --frontend F fuse3: mount through the kernel (default)\n"
           "                   native: serve the client protocol at Unix socket <root>\n"
           "                   preload: native service used through the POSIX interposer\n"
           "  -c, --config F   read dagonfs_model, dagonfs_root_dir, fuse_mode, enable_logging from F\n"
           "  -f               run FUSE in the foreground (default; required under MPI)\n"
           "  -d               foreground with a trace of every FUSE request\n"
           "      --log        verbose logging (legacy form: trailing 1)\n"
           "  -h, --help       show this help\n"
           "\n"
           "Command-line flags override the configuration file. DAGonFS_CS.exe and\n"
           "DAGonFS_P2P.exe are aliases that default to their model.\n"
           "Stop the volume with: fusermount3 -u <mount point>\n";
}

} // namespace dagonfs
