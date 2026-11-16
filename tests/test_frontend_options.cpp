#include "../src/volume/Options.hpp"
#include <cassert>
#include <string>
using namespace dagonfs;
int main() {
    assert(parseFrontend("fuse3") == Frontend::Fuse);
    assert(parseFrontend("native") == Frontend::Native);
    assert(parseFrontend("mpi-io") == Frontend::Native);
    assert(parseFrontend("ld-preload") == Frontend::Preload);
    assert(!parseFrontend("unknown"));
    VolumeOptions o;
    IniValues values{{"dagonfs_frontend", "native"}, {"dagonfs_root_dir", "/tmp/dagonfs.sock"}};
    std::string error;
    assert(applyIni(values, o, error));
    assert(o.frontend == Frontend::Native && o.root == "/tmp/dagonfs.sock");
    assert(o.mounts(0) && !o.mounts(1));
    return 0;
}
