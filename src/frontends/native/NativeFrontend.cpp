#include "NativeFrontend.hpp"
#include "Protocol.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <map>
#include <csignal>
#include <iterator>
#include <poll.h>
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

struct OpenFile { fuse_ino_t ino; int flags; int connection; };
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

} // namespace

INode *NativeFrontend::resolve(const std::string &path, fuse_ino_t *number) {
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

namespace {
int parentOf(const std::string &path, Directory *&parent, std::string &name) {
    auto parts = components(path);
    if (parts.empty()) return EINVAL;
    name = parts.back(); parts.pop_back();
    if (name == "." || name == "..") return EINVAL;
    if (const int error = NativeFrontend::validateName(name)) return error;
    std::string parentPath = "/";
    for (const auto &part : parts) parentPath += part + "/";
    INode *node = NativeFrontend::resolve(parentPath);
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

int NativeFrontend::validateName(const std::string &name) {
    return FileSystem::ValidateName(name.c_str());
}

int NativeFrontend::makeDirectory(const std::string &path, uint32_t mode) {
    Directory *parent = nullptr; std::string name;
    if (const int error = parentOf(path, parent, name)) return error;
    if (parent->ChildINodeNumberWithName(name) != static_cast<fuse_ino_t>(-1)) return EEXIST;
    const fuse_ino_t ino = FileSystem::RegisterINode(DIRECTORY, S_IFDIR | (mode & 07777), 2, getgid(), getuid());
    Directory *directory = FileSystem::GetDirectory(ino);
    directory->setParent(parent);
    directory->setDirName((parent->getDirName() + name + "/").c_str());
    parent->UpdateChild(name, ino);
    parent->AddHardLink();
    FileSystem::Touch(parent, true);
    return 0;
}

// Mirrors FileSystem::FuseUnlink and FileSystem::FuseRmdir.
int NativeFrontend::removeEntry(const std::string &path, bool directory) {
    Directory *parent = nullptr; std::string name;
    if (const int error = parentOf(path, parent, name)) return error;
    const fuse_ino_t ino = parent->ChildINodeNumberWithName(name);
    INode *node = ino == static_cast<fuse_ino_t>(-1) ? nullptr : FileSystem::GetINode(ino);
    if (!node) return ENOENT;
    auto *dir = dynamic_cast<Directory *>(node);
    if (!directory) {
        if (dir) return EISDIR;
        parent->DeleteChild(name);
        FileSystem::Touch(parent, true);
        FileSystem::DropLink(node, ino);
        return 0;
    }
    if (!dir) return ENOTDIR;
    if (dir->hasChildren()) return ENOTEMPTY;
    parent->RemoveHardLink();
    while (!dir->HasNoLinks()) dir->RemoveHardLink();
    parent->DeleteChild(name);
    FileSystem::Touch(parent, true);
    return 0;
}

// Mirrors FileSystem::FuseRename (without RENAME_NOREPLACE).
int NativeFrontend::renameEntry(const std::string &from, const std::string &to) {
    Directory *parent = nullptr, *newParent = nullptr; std::string name, newName;
    if (const int error = parentOf(from, parent, name)) return error;
    if (const int error = parentOf(to, newParent, newName)) return error;
    const fuse_ino_t ino = parent->ChildINodeNumberWithName(name);
    INode *node = ino == static_cast<fuse_ino_t>(-1) ? nullptr : FileSystem::GetINode(ino);
    if (!node) return ENOENT;
    const bool sourceIsDir = dynamic_cast<Directory *>(node) != nullptr;
    const fuse_ino_t existingIno = newParent->ChildINodeNumberWithName(newName);
    INode *existing = existingIno == static_cast<fuse_ino_t>(-1) ? nullptr : FileSystem::GetINode(existingIno);
    if (existingIno == ino) return 0;
    if (sourceIsDir) {
        for (Directory *ancestor = newParent; ancestor; ancestor = ancestor->getParent()) {
            if (ancestor == node) return EINVAL;
            if (ancestor->getParent() == ancestor) break;
        }
    }
    if (existing) {
        auto *existingDir = dynamic_cast<Directory *>(existing);
        if (sourceIsDir && !existingDir) return ENOTDIR;
        if (!sourceIsDir && existingDir) return EISDIR;
        if (existingDir && existingDir->hasChildren()) return ENOTEMPTY;
        newParent->DeleteChild(newName);
        if (existingDir) {
            newParent->RemoveHardLink();
            while (!existing->HasNoLinks()) existing->RemoveHardLink();
        } else {
            FileSystem::DropLink(existing, existingIno);
        }
    }
    if (sourceIsDir && parent != newParent) { parent->RemoveHardLink(); newParent->AddHardLink(); }
    parent->DeleteChild(name);
    newParent->UpdateChild(newName, ino);
    FileSystem::Touch(parent, true);
    FileSystem::Touch(newParent, true);
    FileSystem::Touch(node, false);
    if (sourceIsDir) FileSystem::UpdateDirectoryPaths(dynamic_cast<Directory *>(node), newParent, newParent->getDirName() + newName + "/");
    return 0;
}

NativeFrontend::NativeFrontend(int rank, int worldSize) {
    static FileSystem filesystem(rank, worldSize, Model::ClientServer);
    (void)filesystem;
    struct fuse_conn_info connection{};
    FileSystem::FuseInit(nullptr, &connection);
    auto *root = FileSystem::GetDirectory(FUSE_ROOT_ID);
    if (root) root->setParent(root);
}

namespace {

// One reply per request. A dead peer must not raise SIGPIPE in the volume.
bool sendFull(int fd, const void *data, size_t size) {
    const char *cursor = static_cast<const char *>(data);
    while (size) {
#ifdef MSG_NOSIGNAL
        const ssize_t count = send(fd, cursor, size, MSG_NOSIGNAL);
#else
        const ssize_t count = send(fd, cursor, size, 0);
#endif
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        cursor += count; size -= static_cast<size_t>(count);
    }
    return true;
}

} // namespace

void NativeFrontend::closeHandle(int64_t handle) {
    auto it = handles.find(handle);
    if (it == handles.end()) return;
    auto *file = dynamic_cast<File *>(FileSystem::GetINode(it->second.ino));
    if (file && file->m_openCount) --file->m_openCount;
    if (file && file->m_openCount == 0) { FileSystem::StoreFileContent(file, it->second.ino); file->releaseBuffer(); }
    handles.erase(it);
}

int NativeFrontend::serve(const std::string &socketPath) {
    if (socketPath.size() >= sizeof(sockaddr_un::sun_path)) {
        std::cerr << "DAGonFS native socket path is too long: " << socketPath << std::endl;
        return ENAMETOOLONG;
    }
    signal(SIGPIPE, SIG_IGN);
    const int listener = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listener < 0) return errno;
    sockaddr_un address{}; address.sun_family = AF_UNIX; strcpy(address.sun_path, socketPath.c_str());
    unlink(socketPath.c_str());
    if (::bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) || listen(listener, 64)) {
        const int error = errno; close(listener); return error;
    }
    // Clients keep one connection open and send any number of requests on it.
    // Handles opened on a connection are closed (and written back) when it ends.
    std::vector<pollfd> polled{{listener, POLLIN, 0}};
    bool running = true;
    while (running) {
        for (auto &entry : polled) entry.revents = 0;
        if (poll(polled.data(), polled.size(), -1) < 0) { if (errno == EINTR) continue; break; }
        if (polled[0].revents & POLLIN) {
            const int client = accept(listener, nullptr, nullptr);
            if (client >= 0) {
#ifdef SO_NOSIGPIPE
                const int one = 1; setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
                polled.push_back({client, POLLIN, 0});
            }
        }
        for (size_t index = 1; index < polled.size() && running; ++index) {
            const int client = polled[index].fd;
            if (!polled[index].revents) continue;
            bool keep = (polled[index].revents & POLLIN) && serveRequest(client, running);
            if (!keep) {
                for (auto it = handles.begin(); it != handles.end();) {
                    auto next = std::next(it);
                    if (it->second.connection == client) closeHandle(it->first);
                    it = next;
                }
                close(client);
                polled[index].fd = -1;
            }
        }
        polled.erase(std::remove_if(polled.begin() + 1, polled.end(), [](const pollfd &p) { return p.fd < 0; }), polled.end());
    }
    for (size_t index = 1; index < polled.size(); ++index) close(polled[index].fd);
    while (!handles.empty()) closeHandle(handles.begin()->first);
    close(listener); unlink(socketPath.c_str());
    RequestSender::sendTerminationRequest(0,FileSystem::getMpiWorldSize(),false);
    return 0;
}

bool NativeFrontend::serveRequest(int client, bool &running) {
        dagonfs_native_request q{};
        dagonfs_native_reply r{}; r.magic=DAGONFS_NATIVE_MAGIC; r.version=DAGONFS_NATIVE_VERSION;
        std::string path, second, input, output;
        if (!readFull(client,&q,sizeof(q))) return false;
        if (q.magic!=DAGONFS_NATIVE_MAGIC || q.version!=DAGONFS_NATIVE_VERSION ||
            q.path_length>=DAGONFS_NATIVE_MAX_PATH || q.second_path_length>=DAGONFS_NATIVE_MAX_PATH || q.data_length>DAGONFS_NATIVE_MAX_IO) {
            r.error=EPROTO;
            sendFull(client,&r,sizeof(r));
            return false;
        }
        path.resize(q.path_length); second.resize(q.second_path_length); input.resize(q.data_length);
        if ((q.path_length&&!readFull(client,path.data(),path.size())) ||
            (q.second_path_length&&!readFull(client,second.data(),second.size())) ||
            (q.data_length&&!readFull(client,input.data(),input.size()))) return false;
        switch (q.opcode) {
        case DAGONFS_NATIVE_OPEN: {
            fuse_ino_t ino=0; INode *node=NativeFrontend::resolve(path,&ino);
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
            if (!r.error) { file->m_openCount++; const int64_t h=nextHandle++; handles[h]={ino,q.flags,client}; r.value=h; }
            break;
        }
        case DAGONFS_NATIVE_CLOSE: {
            auto it=handles.find(q.handle); if(it==handles.end()){r.error=EBADF;break;}
            closeHandle(q.handle); break;
        }
        case DAGONFS_NATIVE_READ:
        case DAGONFS_NATIVE_WRITE: {
            auto it=handles.find(q.handle); if(it==handles.end()){r.error=EBADF;break;}
            auto *file=dynamic_cast<File *>(FileSystem::GetINode(it->second.ino)); if(!file){r.error=EBADF;break;}
            const int access=it->second.flags&O_ACCMODE;
            if((q.opcode==DAGONFS_NATIVE_READ&&access==O_WRONLY)||(q.opcode==DAGONFS_NATIVE_WRITE&&access==O_RDONLY)){r.error=EBADF;break;}
            if(q.offset<0){r.error=EINVAL;break;} if(!FileSystem::LoadFileContent(file,it->second.ino)){r.error=ENOMEM;break;}
            size_t offset=static_cast<size_t>(q.offset);
            if(q.opcode==DAGONFS_NATIVE_READ){
                size_t requested=std::min<size_t>(static_cast<uint32_t>(q.flags),DAGONFS_NATIVE_MAX_IO); size_t size=static_cast<size_t>(file->m_fuseEntryParam.attr.st_size);
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
        case DAGONFS_NATIVE_STAT: { INode *node=NativeFrontend::resolve(path);if(!node)r.error=ENOENT;else replyStat(r,node->m_fuseEntryParam.attr);break; }
        case DAGONFS_NATIVE_MKDIR: r.error=makeDirectory(path,q.mode); break;
        case DAGONFS_NATIVE_UNLINK: r.error=removeEntry(path,false); break;
        case DAGONFS_NATIVE_RMDIR: r.error=removeEntry(path,true); break;
        case DAGONFS_NATIVE_RENAME: r.error=renameEntry(path,second); break;
        case DAGONFS_NATIVE_LIST: { auto *d=dynamic_cast<Directory *>(NativeFrontend::resolve(path));if(!d)r.error=ENOTDIR;else{for(auto const &[name,ino]:d->Children()){(void)ino;output+=name+'\n';}r.value=static_cast<int64_t>(output.size());if(output.size()>static_cast<size_t>(q.flags))r.error=ERANGE;else r.data_length=output.size();}break; }
        case DAGONFS_NATIVE_FSTAT: { auto it=handles.find(q.handle);if(it==handles.end()){r.error=EBADF;break;}INode *node=FileSystem::GetINode(it->second.ino);if(!node)r.error=EBADF;else replyStat(r,node->m_fuseEntryParam.attr);break; }
        case DAGONFS_NATIVE_SHUTDOWN: running=false; break;
        default: r.error=ENOSYS; break;
        }
        if (!sendFull(client,&r,sizeof(r))) return false;
        if (!r.error && r.data_length && !sendFull(client,output.data(),output.size())) return false;
        return true;
}
} // namespace dagonfs
