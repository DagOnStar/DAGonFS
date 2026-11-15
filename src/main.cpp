//
// dagonfs: create a DAGonFS volume over the ranks of an MPI job.
//
//   mpirun -np <P> dagonfs [--model client-server|peer-to-peer] [--config DAGonFS.ini] [-f|-d] [--log] <root>
//

#include <cstdlib>
#include <iostream>
#include <string>

#include <mpi.h>

#include "volume/Volume.hpp"

int main(int argc, char *argv[]) {
    // Parse before MPI_Init: the model decides the MPI thread level. argv is
    // only read, never modified.
    dagonfs::VolumeOptions options;
    std::string error;
    const std::string program = argc > 0 ? argv[0] : "dagonfs";
    switch (dagonfs::parseCommandLine(argc, argv, options, error)) {
        case dagonfs::ParseStatus::Help:
            std::cout << dagonfs::usage(program);
            return 0;
        case dagonfs::ParseStatus::Error:
            std::cerr << program << ": " << error << "\n" << dagonfs::usage(program);
            return 1;
        case dagonfs::ParseStatus::Ok:
            break;
    }

    // DAGonFS uses point-to-point messages only, never collectives. Open MPI's
    // Mellanox HCOLL component adds nothing but a teardown in MPI_Finalize
    // that was observed to deadlock intermittently across nodes (D47). An
    // explicit setting in the environment still takes precedence.
    setenv("OMPI_MCA_coll_hcoll_enable", "0", 0);
    int provided = MPI_THREAD_SINGLE;
    MPI_Init_thread(&argc, &argv, dagonfs::requiredThreadLevel(options.model), &provided);
    const int status = dagonfs::runVolume(options);
    MPI_Finalize();
    return status;
}
