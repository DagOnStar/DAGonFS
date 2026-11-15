//
// DAGonFS volume API: create (mount and serve) a volume from an MPI program.
//
//   #include <dagonfs/Volume.hpp>
//
//   dagonfs::VolumeOptions options;
//   options.model = dagonfs::Model::PeerToPeer;
//   options.root  = "/tmp/dagonfs";
//   int provided;
//   MPI_Init_thread(&argc, &argv, dagonfs::requiredThreadLevel(options.model), &provided);
//   int status = dagonfs::runVolume(options);   // returns once the volume is unmounted
//   MPI_Finalize();
//

#ifndef DAGONFS_VOLUME_HPP
#define DAGONFS_VOLUME_HPP

#include "Options.hpp"

namespace dagonfs {

/** MPI thread-support level a model needs (MPI_THREAD_MULTIPLE for peer-to-peer). */
int requiredThreadLevel(Model model);

/**
 * Run this rank's part of a volume. Collective over MPI_COMM_WORLD: every rank
 * must call it with the same options. MPI must already be initialised with at
 * least requiredThreadLevel(options.model).
 *
 * Ranks that mount the volume (VolumeOptions::mounts) serve FUSE requests
 * until their mount point is unmounted; the other ranks serve data-plane
 * requests until the front end terminates. Unmounting the client-server front
 * end, or any one peer, shuts the whole volume down.
 *
 * @return 0 after a clean shutdown, otherwise a non-zero error code.
 */
int runVolume(const VolumeOptions &options);

} // namespace dagonfs

#endif // DAGONFS_VOLUME_HPP
