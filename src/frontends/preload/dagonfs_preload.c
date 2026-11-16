#define _GNU_SOURCE
#include "../native/Client.h"
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

/* Opt-in interposer. It only handles paths below DAGONFS_PRELOAD_ROOT; all
 * other calls are delegated to libc. This keeps normal applications safe. */
static const char *root(void) { return getenv("DAGONFS_PRELOAD_ROOT"); }
static int remote_path(const char *p, const char **out) {
    const char *r=root(); size_t n;
    if (!r || !p || p[0] != '/') return 0;
    n=strlen(r); if (strncmp(p,r,n)!=0 || (p[n] && p[n]!='/')) return 0;
    *out=p+n; if (!**out) *out="/"; return 1;
}
struct slot { int fd; int64_t h; }; static struct slot slots[1024]; static pthread_mutex_t mu=PTHREAD_MUTEX_INITIALIZER;
static int alloc(int64_t h){ int i; pthread_mutex_lock(&mu); for(i=0;i<1024;i++) if(!slots[i].fd){slots[i].fd=100000+i;slots[i].h=h;pthread_mutex_unlock(&mu);return slots[i].fd;} pthread_mutex_unlock(&mu); errno=EMFILE; return -1; }
static int64_t findh(int fd){int i; for(i=0;i<1024;i++)if(slots[i].fd==fd)return slots[i].h;return -1;}
static void release(int fd){int i;pthread_mutex_lock(&mu);for(i=0;i<1024;i++)if(slots[i].fd==fd)slots[i].fd=0;pthread_mutex_unlock(&mu);}
int open(const char *p,int flags,...){ static int(*real_open)(const char*,int,...); const char *q; mode_t m=0; va_list ap; int64_t h; if(!real_open)real_open=dlsym(RTLD_NEXT,"open"); if(!remote_path(p,&q)){va_start(ap,flags);m=va_arg(ap,int);va_end(ap);return real_open(p,flags,m);} h=dagonfs_client_open(q,flags,0666); if(h<0)return -1; return alloc(h); }
int close(int fd){int64_t h=findh(fd); if(h<0){static int(*f)(int);if(!f)f=dlsym(RTLD_NEXT,"close");return f(fd);} release(fd);return dagonfs_client_close(h);}
ssize_t pread(int fd,void*b,size_t n,off_t o){int64_t h=findh(fd);if(h<0){static ssize_t(*f)(int,void*,size_t,off_t);if(!f)f=dlsym(RTLD_NEXT,"pread");return f(fd,b,n,o);}return dagonfs_client_pread(h,b,n,o);}
ssize_t pwrite(int fd,const void*b,size_t n,off_t o){int64_t h=findh(fd);if(h<0){static ssize_t(*f)(int,const void*,size_t,off_t);if(!f)f=dlsym(RTLD_NEXT,"pwrite");return f(fd,b,n,o);}return dagonfs_client_pwrite(h,b,n,o);}
ssize_t read(int fd,void*b,size_t n){static off_t(*seek)(int,off_t,int);int64_t h=findh(fd);if(h<0){static ssize_t(*f)(int,void*,size_t);if(!f)f=dlsym(RTLD_NEXT,"read");return f(fd,b,n);}if(!seek)seek=dlsym(RTLD_NEXT,"lseek");off_t o=seek(fd,0,SEEK_CUR);ssize_t z=dagonfs_client_pread(h,b,n,o);if(z>=0)seek(fd,o+z,SEEK_SET);return z;}
ssize_t write(int fd,const void*b,size_t n){static off_t(*seek)(int,off_t,int);int64_t h=findh(fd);if(h<0){static ssize_t(*f)(int,const void*,size_t);if(!f)f=dlsym(RTLD_NEXT,"write");return f(fd,b,n);}if(!seek)seek=dlsym(RTLD_NEXT,"lseek");off_t o=seek(fd,0,SEEK_CUR);ssize_t z=dagonfs_client_pwrite(h,b,n,o);if(z>=0)seek(fd,o+z,SEEK_SET);return z;}
