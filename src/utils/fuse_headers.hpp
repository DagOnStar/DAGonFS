//
// Created by frank on 1/2/25.
//

#ifndef FUSE_HEADERS_HPP
#define FUSE_HEADERS_HPP

#ifdef __APPLE__
#if __has_include(<osxfuse/fuse/fuse_lowlevel.h>)
#include <osxfuse/fuse/fuse_lowlevel.h>
#else
#include <fuse3/fuse_lowlevel.h>
#endif
#else
#include <fuse3/fuse_lowlevel.h>
#endif

#endif //MY_FUSE_HEADERS_HPP
