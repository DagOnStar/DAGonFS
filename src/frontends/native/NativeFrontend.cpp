#include "NativeFrontend.hpp"
#include "Protocol.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <map>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

#include "../../nodes/Directory.hpp"
#include "../../nodes/File.hpp"
#include "../../mpi/RequestSender.hpp"
#include "../../ramfs/FileSystem.hpp"

namespace dagonfs {
namespace {

bool readFull(int fd, void *data, size_t size) {
    char *cursor = static_cast<char *>(data);
    while (size) {
        const ssize_t count = read(fd, cursor, size);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        cursor += count; size -= static_cast<size_t>(count);
    }
    return true;
}
bool writeFull(int fd, const void *data, size_t size) {
    const char *cursor = static_cast<const char *>(data);
    while (size) {
        const ssize_t count = write(fd, cursor, size);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        cursor += count; size -= static_cast<size_t>(count);
    }
    return true;
}

struct OpenFile { fuse_ino_t ino; int flags; };
std::map<int64_t, OpenFile> handles;
int64_t nextHandle = 1;

std::vector<std::string> components(const std::string &path) {
    std::vector<std::string> result;
    size_t begin = 0;
    while (begin < path.size()) {
        while (begin < path.size() && path[begin] == '/') ++begin;
        const size_t end = path.find('/', begin);
        if (begin < path.size()) result.push_back(path.substr(begin, end - begin));
        if (end == std::string::npos) break;
        begin = end + 1;
    }
    return result;
}

INode *resolve(const std::string &path, fuse_ino_t *number = nullptr) {
    fuse_ino_t ino = FUSE_ROOT_ID;
    INode *node = FileSystem::GetINode(ino);
    for (const auto &part : components(path)) {
        if (part == ".") continue;
        if (part == "..") {
            auto *dir = dynamic_cast<Directory *>(node);
            if (!dir) return nullptr;
            node = dir->getParent() ? dir->getParent() : dir;
            ino = node->inodeNumber;
            continue;
        }
        auto *dir = dynamic_cast<Directory *>(node);
        if (!dir) return nullptr;
        ino = dir->ChildINodeNumberWithName(part);
        if (ino == static_cast<fuse_ino_t>(-1)) return nullptr;
        node = FileSystem::GetINode(ino);
        if (!node) return nullptr;
    }
    if (number) *number = ino;
    return node;
}

int parentOf(const std::string &path, Directory *&parent, std::string &name) {
    auto parts = components(path);
    if (parts.empty()) return EINVAL;
    name = parts.back(); parts.pop_back();
    std::string parentPath = "/";
    for (const auto &part : parts) parentPath += part + "/";
    INode *node = resolve(parentPath);
    if (!node) return ENOENT;
    parent = dynamic_cast<Directory *>(node);
    return parent ? 0 : ENOTDIR;
}

void replyStat(dagonfs_native_reply &reply, const struct stat &s) {
    reply.inode=s.st_ino; reply.size=s.st_size; reply.mode=s.st_mode; reply.nlink=s.st_nlink; reply.uid=s.st_uid; reply.gid=s.st_gid;
#ifdef __APPLE__
    reply.atime_sec=s.st_atimespec.tv_sec; reply.mtime_sec=s.st_mtimespec.tv_sec; reply.ctime_sec=s.st_ctimespec.tv_sec;
#else
    reply.atime_sec=s.st_atim.tv_sec; reply.mtime_sec=s.st_mtim.tv_sec; reply.ctime_sec=s.st_ctim.tv_sec;
#endif
}

} // namespace

NativeFrontend::NativeFrontend(int rank, int worldSize) {
    static FileSystem filesystem(rank, worldSize, Model::ClientServer);
    (void)filesystem;
    struct fuse_conn_info connection{};
    FileSystem::FuseInit(nullptr, &connection);
    auto *root = FileSystem::GetDirectory(FUSE_ROOT_ID);
    if (root) root->setParent(root);
}

int NativeFrontend::serve(const std::string &socketPath) {
    if (socketPath.size() >= sizeof(sockaddr_un::sun_path)) {
        std::cerr << "DAGonFS native socket path is too long: " << socketPath << std::endl;
        return ENAMETOOLONG;
    }
    const int listener = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listener < 0) return errno;
    sockaddr_un address{}; address.sun_family = AF_UNIX; strcpy(address.sun_path, socketPath.c_str());
    unlink(socketPath.c_str());
    if (bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) || listen(listener, 64)) {
        const int error = errno; close(listener); return error;
    }
    bool running = true;
    while (running) {
        const int client = accept(listener, nullptr, nullptr);
        if (client < 0) { if (errno == EINTR) continue; break; }
        dagonfs_native_request q{};
        dagonfs_native_reply r{}; r.magic=DAGONFS_NATIVE_MAGIC; r.version=DAGONFS_NATIVE_VERSION;
        std::string path, second, input, output;
        if (!readFull(client,&q,sizeof(q)) || q.magic!=DAGONFS_NATIVE_MAGIC || q.version!=DAGONFS_NATIVE_VERSION ||
            q.path_length>=DAGONFS_NATIVE_MAX_PATH || q.second_path_length>=DAGONFS_NATIVE_MAX_PATH || q.data_length>DAGONFS_NATIVE_MAX_IO) {
            r.error=EPROTO;
        } else {
            path.resize(q.path_length); second.resize(q.second_path_length); input.resize(q.data_length);
            if ((q.path_length&&!readFull(client,path.data(),path.size())) ||
                (q.second_path_length&&!readFull(client,second.data(),second.size())) ||
                (q.data_length&&!readFull(client,input.data(),input.size()))) r.error=EPROTO;
        }
        if (!r.error) switch (q.opcode) {
        case DAGONFS_NATIVE_OPEN: {
            fuse_ino_t ino=0; INode *node=resolve(path,&ino);
            if (!node && (q.flags&O_CREAT)) {
                Directory *parent=nullptr; std::string name; r.error=parentOf(path,parent,name);
                if (!r.error && parent->ChildINodeNumberWithName(name)!=static_cast<fuse_ino_t>(-1)) r.error=EEXIST;
                if (!r.error) {
                    ino=FileSystem::RegisterINode(REGULAR_FILE,S_IFREG|(q.mode&07777),1,getgid(),getuid());
                    auto *file=dynamic_cast<File *>(FileSystem::GetINode(ino));
                    parent->UpdateChild(name,ino); FileSystem::Touch(parent,true); FileSystem::AssignGlobalId(file,ino,false); node=file;
                }
            } else if (!node) r.error=ENOENT;
            else if ((q.flags&O_CREAT)&&(q.flags&O_EXCL)) r.error=EEXIST;
            if (!r.error && dynamic_cast<Directory *>(node)) r.error=EISDIR;
            auto *file=dynamic_cast<File *>(node);
            if (!r.error && !file) r.error=EINVAL;
            if (!r.error) {
                if (q.flags&O_TRUNC) { FileSystem::SetSize(file,0); file->setWaiting(); }
                else if (!FileSystem::LoadFileContent(file,ino)) r.error=ENOMEM;
            }
            if (!r.error) { file->m_openCount++; const int64_t h=nextHandle++; handles[h]={ino,q.flags}; r.value=h; }
            break;
        }
        case DAGONFS_NATIVE_CLOSE: {
            auto it=handles.find(q.handle); if(it==handles.end()){r.error=EBADF;break;}
            auto *file=dynamic_cast<File *>(FileSystem::GetINode(it->second.ino));
            if(file&&file->m_openCount) --file->m_openCount;
            if(file&&file->m_openCount==0){FileSystem::StoreFileContent(file,it->second.ino);file->releaseBuffer();}
            handles.erase(it); break;
        }
        case DAGONFS_NATIVE_READ:
        case DAGONFS_NATIVE_WRITE: {
            auto it=handles.find(q.handle); if(it==handles.end()){r.error=EBADF;break;}
            auto *file=dynamic_cast<File *>(FileSystem::GetINode(it->second.ino)); if(!file){r.error=EBADF;break;}
            if(q.offset<0){r.error=EINVAL;break;} if(!FileSystem::LoadFileContent(file,it->second.ino)){r.error=ENOMEM;break;}
            size_t offset=static_cast<size_t>(q.offset);
            if(q.opcode==DAGONFS_NATIVE_READ){
                size_t requested=static_cast<uint32_t>(q.flags); size_t size=static_cast<size_t>(file->m_fuseEntryParam.attr.st_size);
                size_t count=offset>=size?0:std::min(requested,size-offset); output.assign(static_cast<char *>(file->m_buf)+offset,count); r.data_length=count;
            }else{
                if(it->second.flags&O_APPEND) offset=static_cast<size_t>(file->m_fuseEntryParam.attr.st_size);
                if(input.size()>std::numeric_limits<size_t>::max()-offset){r.error=EFBIG;break;}
                size_t end=offset+input.size(); if(!file->ensureCapacity(end)){r.error=ENOMEM;break;}
                memcpy(static_cast<char *>(file->m_buf)+offset,input.data(),input.size());
                if(end>static_cast<size_t>(file->m_fuseEntryParam.attr.st_size))FileSystem::SetSize(file,end);
                file->setWaiting();FileSystem::Touch(file,true);r.value=input.size();
            } break;
        }
        case DAGONFS_NATIVE_FSYNC: { auto it=handles.find(q.handle);if(it==handles.end()){r.error=EBADF;break;}auto *f=dynamic_cast<File *>(FileSystem::GetINode(it->second.ino));if(!f)r.error=EBADF;else FileSystem::StoreFileContent(f,it->second.ino);break; }
        case DAGONFS_NATIVE_STAT: { INode *node=resolve(path);if(!node)r.error=ENOENT;else replyStat(r,node->m_fuseEntryParam.attr);break; }
        case DAGONFS_NATIVE_MKDIR: { Directory *parent=nullptr;std::string name;r.error=parentOf(path,parent,name);if(!r.error&&parent->ChildINodeNumberWithName(name)!=static_cast<fuse_ino_t>(-1))r.error=EEXIST;if(!r.error){auto ino=FileSystem::RegisterINode(DIRECTORY,S_IFDIR|(q.mode&07777),2,getgid(),getuid());auto *d=FileSystem::GetDirectory(ino);d->setParent(parent);d->setDirName((parent->getDirName()+name+"/").c_str());parent->UpdateChild(name,ino);parent->AddHardLink();}break; }
        case DAGONFS_NATIVE_UNLINK: { Directory *parent=nullptr;std::string name;r.error=parentOf(path,parent,name);if(!r.error){auto ino=parent->ChildINodeNumberWithName(name);auto *node=ino==static_cast<fuse_ino_t>(-1)?nullptr:FileSystem::GetINode(ino);if(!node)r.error=ENOENT;else if(dynamic_cast<Directory *>(node))r.error=EISDIR;else{parent->DeleteChild(name);FileSystem::DropLink(node,ino);}}break; }
        case DAGONFS_NATIVE_RMDIR: { Directory *parent=nullptr;std::string name;r.error=parentOf(path,parent,name);if(!r.error){auto ino=parent->ChildINodeNumberWithName(name);auto *d=ino==static_cast<fuse_ino_t>(-1)?nullptr:FileSystem::GetDirectory(ino);if(!d)r.error=ENOENT;else if(d->hasChildren())r.error=ENOTEMPTY;else{parent->DeleteChild(name);parent->RemoveHardLink();}}break; }
        case DAGONFS_NATIVE_RENAME: { Directory *a=nullptr,*b=nullptr;std::string an,bn;r.error=parentOf(path,a,an);if(!r.error)r.error=parentOf(second,b,bn);if(!r.error){auto ino=a->ChildINodeNumberWithName(an);if(ino==static_cast<fuse_ino_t>(-1))r.error=ENOENT;else{auto old=b->ChildINodeNumberWithName(bn);if(old!=static_cast<fuse_ino_t>(-1)){auto *n=FileSystem::GetINode(old);if(dynamic_cast<Directory *>(n)){auto *d=dynamic_cast<Directory *>(n);if(d->hasChildren()){r.error=ENOTEMPTY;break;}}b->DeleteChild(bn);FileSystem::DropLink(n,old);}a->DeleteChild(an);b->UpdateChild(bn,ino);if(auto*d=FileSystem::GetDirectory(ino))FileSystem::UpdateDirectoryPaths(d,b,b->getDirName()+bn+"/");}}break; }
        case DAGONFS_NATIVE_LIST: { auto *d=dynamic_cast<Directory *>(resolve(path));if(!d)r.error=ENOTDIR;else{for(auto const &[name,ino]:d->Children()){(void)ino;output+=name+'\n';}if(output.size()>static_cast<size_t>(q.flags))r.error=ERANGE;else r.data_length=output.size();}break; }
        case DAGONFS_NATIVE_SHUTDOWN: running=false; break;
        default: r.error=ENOSYS; break;
        }
        writeFull(client,&r,sizeof(r)); if(!r.error&&r.data_length)writeFull(client,output.data(),output.size()); close(client);
    }
    close(listener); unlink(socketPath.c_str());
    RequestSender::sendTerminationRequest(0,FileSystem::getMpiWorldSize(),false);
    return 0;
}
} // namespace dagonfs
