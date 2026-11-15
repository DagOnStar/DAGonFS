// DAGonFS launcher: reads DAGonFS.ini and starts the selected model under mpirun.
//
// Usage: DAGonFS_Launcher [path/to/DAGonFS.ini] [-n|--dry-run]
//
// The command is executed with execvp (no shell), so configuration values are
// never interpreted by /bin/sh.

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

using namespace std;

namespace {

string trim(const string &value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == string::npos) return "";
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

bool readConfig(const string &filename, map<string, string> &config) {
    ifstream file(filename);
    if (!file) {
        cerr << "Error: unable to open configuration file '" << filename << "'" << endl;
        return false;
    }
    string line;
    while (getline(file, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#' || line[0] == ';' || line[0] == '[') continue;
        const size_t pos = line.find('=');
        if (pos == string::npos) {
            cerr << "Warning: ignoring malformed line '" << line << "'" << endl;
            continue;
        }
        config[trim(line.substr(0, pos))] = trim(line.substr(pos + 1));
    }
    return true;
}

string value(const map<string, string> &config, const string &key, const string &fallback = "") {
    const auto entry = config.find(key);
    return entry == config.end() ? fallback : entry->second;
}

} // namespace

int main(int argc, char *argv[]) {
    string configFile = "DAGonFS.ini";
    bool dryRun = false;
    for (int i = 1; i < argc; ++i) {
        const string arg = argv[i];
        if (arg == "-n" || arg == "--dry-run") {
            dryRun = true;
        } else if (arg == "-h" || arg == "--help") {
            cout << "Usage: " << argv[0] << " [DAGonFS.ini] [-n|--dry-run]" << endl;
            return 0;
        } else {
            configFile = arg;
        }
    }

    map<string, string> config;
    if (!readConfig(configFile, config)) {
        return 1;
    }

    int numProcesses = 0;
    try {
        size_t consumed = 0;
        numProcesses = stoi(value(config, "num_processes"), &consumed);
        if (consumed != value(config, "num_processes").size()) throw invalid_argument("trailing characters");
    } catch (const exception &) {
        cerr << "Error: num_processes must be a positive integer" << endl;
        return 1;
    }
    if (numProcesses < 1) {
        cerr << "Error: num_processes must be a positive integer" << endl;
        return 1;
    }

    const string model = value(config, "dagonfs_model", "client-server");
    string executable;
    if (model == "client-server") {
        executable = "./DAGonFS_CS.exe";
    } else if (model == "peer-to-peer") {
        executable = "./DAGonFS_P2P.exe";
    } else {
        cerr << "Error: dagonfs_model must be 'client-server' or 'peer-to-peer', not '" << model << "'" << endl;
        return 1;
    }

    const string rootDir = value(config, "dagonfs_root_dir");
    if (rootDir.empty()) {
        cerr << "Error: dagonfs_root_dir is required" << endl;
        return 1;
    }
    string fuseMode = value(config, "fuse_mode", "f");
    if (fuseMode != "f" && fuseMode != "d") {
        cerr << "Error: fuse_mode must be 'f' (foreground) or 'd' (debug)" << endl;
        return 1;
    }
    const string enableLogging = value(config, "enable_logging", "false");
    const string machinefile = value(config, "machinefile");
    const string mpirun = value(config, "mpirun", "mpirun");

    vector<string> command = {mpirun, "-np", to_string(numProcesses)};
    if (!machinefile.empty()) {
        command.push_back("--hostfile");
        command.push_back(machinefile);
    }
    command.push_back(executable);
    command.push_back("-" + fuseMode);
    command.push_back(rootDir);
    command.push_back(enableLogging == "true" || enableLogging == "1" ? "1" : "0");

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
