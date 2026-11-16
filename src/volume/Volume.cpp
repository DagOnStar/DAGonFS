//
// DAGonFS volume API (see Volume.hpp).
//

#include "Volume.hpp"

#include <iostream>
#include <thread>

#include <mpi.h>
#include <sys/stat.h>

#include "../blocks/Blocks.hpp"
#include "../mpi/DataBlockManager.hpp"
#include "../mpi/DistributedCode.hpp"
#include "../nodes/Nodes.hpp"
#include "../ramfs/FileSystem.hpp"
#include "../utils/log_level.hpp"
#include "../frontends/native/NativeFrontend.hpp"

log4cplus::LogLevel DAGONFS_LOG_LEVEL = log4cplus::OFF_LOG_LEVEL;

namespace dagonfs {

int requiredThreadLevel(Model model) {
    // Peers run a listener thread and a FUSE thread that both call MPI.
    return model == Model::PeerToPeer ? MPI_THREAD_MULTIPLE : MPI_THREAD_SINGLE;
}

int runVolume(const VolumeOptions &options) {
    int initialized = 0;
    MPI_Initialized(&initialized);
    if (!initialized) {
        std::cerr << "DAGonFS: runVolume() requires MPI to be initialised" << std::endl;
        return 1;
    }
    int rank = 0, worldSize = 0, provided = MPI_THREAD_SINGLE;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);
    MPI_Query_thread(&provided);
    if (provided < requiredThreadLevel(options.model)) {
        // Every rank detects the same condition, so nobody is left waiting.
        if (rank == 0) {
            std::cerr << "DAGonFS " << modelName(options.model)
                      << " requires an MPI library with MPI_THREAD_MULTIPLE support" << std::endl;
        }
        return 1;
    }
    if (options.root.empty()) {
        if (rank == 0) std::cerr << "DAGonFS: no mount root given" << std::endl;
        return 1;
    }
    if (options.frontend != Frontend::Fuse && options.model == Model::PeerToPeer) {
        if (rank == 0) std::cerr << "DAGonFS: native and preload frontends currently require client-server mode" << std::endl;
        return 1;
    }

    DAGONFS_LOG_LEVEL = options.logging ? log4cplus::ALL_LOG_LEVEL : log4cplus::OFF_LOG_LEVEL;
    log4cplus::Initializer initializer;
    log4cplus::BasicConfigurator logConfiguration;
    logConfiguration.configure();

    // Permission bits come from the FUSE requests, not from this process.
    const mode_t previousUmask = umask(0000);

    Nodes::getInstance();
    Blocks::getInstance();
    DataBlockManager::getInstance(worldSize);
    const bool peerToPeer = options.model == Model::PeerToPeer;
    const std::string mountPoint = options.mountPoint(rank);
    DistributedCode::configure(rank, worldSize, peerToPeer ? mountPoint : std::string());
    DistributedCode *controlPlane = DistributedCode::getInstance();

    int status = 0;
    if (options.frontend != Frontend::Fuse && rank == 0) {
        dagonfs::NativeFrontend frontend(rank, worldSize);
        // Client-server rank zero only originates MPI requests. MPI's
        // single-thread level forbids an additional listener here.
        status = frontend.serve(options.root);
    } else if (!options.mounts(rank)) {
        // Client-server storage rank: serve the front end until TERMINATE.
        controlPlane->start();
    } else if (peerToPeer) {
        std::thread listener(&DistributedCode::start);
        FileSystem fileSystem(rank, worldSize, options.model);
        status = fileSystem.start(mountPoint, options.debug);
        listener.join();
    } else {
        FileSystem fileSystem(rank, worldSize, options.model);
        status = fileSystem.start(mountPoint, options.debug);
    }

    umask(previousUmask);
    return status;
}

} // namespace dagonfs
