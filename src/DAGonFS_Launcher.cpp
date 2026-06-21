#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <map>
#include <cstdlib>

using namespace std;

string trim(string value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == string::npos) return "";
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

map<string, string> readConfig(const string& filename) {
    map<string, string> config;
    ifstream file(filename);
    string line;

    if (!file) {
        cerr << "Error: Unable to open configuration file!" << endl;
        exit(1);
    }

    while (getline(file, line)) {
        if (line.empty() || line[0] == '#') continue; // Ignora linee vuote e commenti
        size_t pos = line.find('=');
        if (pos != string::npos) {
            string key = trim(line.substr(0, pos));
            string value = trim(line.substr(pos + 1));
            config[key] = value;
            cout<<"config["<<key<<"]="<<value<<endl;
        }
    }

    return config;
}

int main() {
    string configFile = "DAGonFS.ini";
    auto config = readConfig(configFile);

    // Recupero parametri
    int num_processes = stoi(config["num_processes"]);
    string machinefile = config["machinefile"];
    string executable;
    if(config["dagonfs_model"] == "client-server"){
      executable = "DAGonFS_CS.exe";}
    else{cout << config["dagonfs_model"]<<"==client-server"<<endl;
      executable = "DAGonFS_P2P.exe";}
    string fuse_mode = config["fuse_mode"];
    string dagonfs_root_dir = config["dagonfs_root_dir"];
    bool enable_logging = (config["enable_logging"] == "true");

    // Costruisco il comando mpirun
    stringstream command;
    command << "mpirun -np " << num_processes;
    if(machinefile.length() != 0)
      command << "--hostfile " << machinefile;
    command << " ./" << executable
            << " -" << fuse_mode
            << " " << dagonfs_root_dir
            << " " << enable_logging;

    // Stampo ed eseguo il comando
    cout << "Eseguendo: " << command.str() << endl;
    int ret = system(command.str().c_str());
    if (ret != 0) {
        cerr << "Error during DAGonFS execution!" << endl;
    }

    return ret;
}
