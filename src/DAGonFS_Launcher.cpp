// DAGonFS launcher: reads DAGonFS.ini and starts a volume under mpirun.
//
// Usage: DAGonFS_Launcher [path/to/DAGonFS.ini] [-m|--model M] [-n|--dry-run]
//
// The command is executed with execvp (no shell), so configuration values are
// never interpreted by /bin/sh.

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "volume/Options.hpp"

using namespace std;

namespace {

string value(const dagonfs::IniValues &config, const string &key, const string &fallback = "") {
    const auto entry = config.find(key);
    return entry == config.end() || entry->second.empty() ? fallback : entry->second;
}

/** dagonfs next to the launcher, if present; otherwise looked up in PATH. */
string defaultExecutable(const char *launcher) {
    const string self = launcher;
    const auto slash = self.find_last_of('/');
    if (slash != string::npos) {
        const string sibling = self.substr(0, slash + 1) + "dagonfs";
        struct stat st{};
        if (stat(sibling.c_str(), &st) == 0) return sibling;
    }
    return "dagonfs";
}

void usage(const char *program) {
    cout << "Usage: " << program << " [DAGonFS.ini] [-m|--model client-server|peer-to-peer] [-n|--dry-run]\n"
            "  Reads the configuration (default ./DAGonFS.ini) and runs\n"
            "  <mpirun> -np <num_processes> [--hostfile <machinefile>] <executable> --model <model> -f|-d [--log] <root>\n"
            "  --model overrides dagonfs_model from the configuration file.\n";
}

} // namespace

int main(int argc, char *argv[]) {
    string configFile = "DAGonFS.ini";
    string modelOverride;
    bool dryRun = false;
    for (int i = 1; i < argc; ++i) {
        const string arg = argv[i];
        if (arg == "-n" || arg == "--dry-run") {
            dryRun = true;
        } else if (arg == "-h" || arg == "--help") {
            usage(argv[0]);
            return 0;
        } else if ((arg == "-m" || arg == "--model") && i + 1 < argc) {
            modelOverride = argv[++i];
        } else if (arg.rfind("--model=", 0) == 0) {
            modelOverride = arg.substr(8);
        } else if (!arg.empty() && arg[0] == '-') {
            cerr << "Error: unknown option '" << arg << "'" << endl;
            usage(argv[0]);
            return 1;
        } else {
            configFile = arg;
        }
    }

    dagonfs::IniValues config;
    dagonfs::VolumeOptions options;
    string error;
    if (!dagonfs::readIni(configFile, config, error) || !dagonfs::applyIni(config, options, error)) {
        cerr << "Error: " << error << endl;
        return 1;
    }
    if (!modelOverride.empty()) {
        const auto model = dagonfs::parseModel(modelOverride);
        if (!model) {
            cerr << "Error: unknown model '" << modelOverride << "'" << endl;
            return 1;
        }
        options.model = *model;
    }
    if (options.root.empty()) {
        cerr << "Error: dagonfs_root_dir is required" << endl;
        return 1;
    }

    int numProcesses = 0;
    try {
        size_t consumed = 0;
        const string text = value(config, "num_processes");
        numProcesses = stoi(text, &consumed);
        if (consumed != text.size()) throw invalid_argument("trailing characters");
    } catch (const exception &) {
        numProcesses = 0;
    }
    if (numProcesses < 1) {
        cerr << "Error: num_processes must be a positive integer" << endl;
        return 1;
    }

    const string machinefile = value(config, "machinefile");
    const string mpirun = value(config, "mpirun", "mpirun");
    const string executable = value(config, "executable", defaultExecutable(argv[0]));

    vector<string> command = {mpirun, "-np", to_string(numProcesses)};
    if (!machinefile.empty()) {
        command.push_back("--hostfile");
        command.push_back(machinefile);
    }
    command.push_back(executable);
    command.push_back("--model");
    command.push_back(dagonfs::modelName(options.model));
    command.push_back(options.debug ? "-d" : "-f");
    if (options.logging) command.push_back("--log");
    command.push_back(options.root);

    cout << "Executing:";
    for (const string &part : command) cout << " " << part;
    cout << endl;
    if (dryRun) {
        return 0;
    }

    vector<char *> args;
    for (string &part : command) args.push_back(part.data());
    args.push_back(nullptr);

    const pid_t child = fork();
    if (child < 0) {
        cerr << "Error: fork failed: " << strerror(errno) << endl;
        return 1;
    }
    if (child == 0) {
        execvp(args[0], args.data());
        cerr << "Error: cannot execute " << args[0] << ": " << strerror(errno) << endl;
        _exit(127);
    }
    int status = 0;
    if (waitpid(child, &status, 0) < 0) {
        cerr << "Error: waitpid failed: " << strerror(errno) << endl;
        return 1;
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        return 0;
    }
    cerr << "Error during DAGonFS execution!" << endl;
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}
