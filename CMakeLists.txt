cmake_minimum_required(VERSION 3.20)

set(PROJECT_NAME DAGonFS)
project(${PROJECT_NAME})

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)

# DAGonFS is intrinsically MPI-distributed; the option is kept for backward
# compatibility with existing build scripts.
option(USE_MPI "Use MPI for distributed memory." ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/CMake)

find_package(FUSE3 REQUIRED)
include_directories(SYSTEM ${FUSE3_INCLUDE_DIR})

find_package(MPI REQUIRED COMPONENTS C)
message(STATUS "Using MPI for distributed memory.")
add_definitions(-DUSE_MPI)
include_directories(SYSTEM ${MPI_C_INCLUDE_DIRS} ${MPI_INCLUDE_PATH})

if(WIN32)
  set(SHAREDIR ".")
  set(DOCDIR "doc")
elseif(APPLE)
  set(SHAREDIR "${CMAKE_INSTALL_PREFIX}/share/${PROJECT_NAME}")
  set(DOCDIR "share/doc/${PROJECT_NAME}")
  set(MANDIR "share/man")
elseif(UNIX) # Linux, BSD etc
  set(SHAREDIR "share/${PROJECT_NAME}")
  set(DOCDIR "share/doc/${PROJECT_NAME}")
  set(MANDIR "share/man")
endif()


add_subdirectory(docs)

#install(DIRECTORY sample_data DESTINATION "${SHAREDIR}")

#log4cplus
#find_package(LOG4CPLUS)
#include_directories(SYSTEM ${LOG4CPLUS_INCLUDE_DIR})
set(EXTERNAL_INSTALL_LOCATION ${CMAKE_BINARY_DIR}/external)
include_directories(SYSTEM ${EXTERNAL_INSTALL_LOCATION}/include)
include(ExternalProject)
link_directories(${EXTERNAL_INSTALL_LOCATION}/lib)
ExternalProject_Add(log4cplus
        #URL https://kumisystems.dl.sourceforge.net/project/log4cplus/log4cplus-stable/2.0.5/log4cplus-2.0.5.tar.gz
        URL https://github.com/log4cplus/log4cplus/releases/download/REL_2_1_1/log4cplus-2.1.1.tar.gz
        TIMEOUT 360
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND ./configure --prefix=${EXTERNAL_INSTALL_LOCATION} CFLAGS=-fPIC CPPFLAGS=-I${EXTERNAL_INSTALL_LOCATION}/include/ LDFLAGS=-L${EXTERNAL_INSTALL_LOCATION}/lib/
        BUILD_COMMAND make
        INSTALL_COMMAND make install
        BUILD_BYPRODUCTS
            ${EXTERNAL_INSTALL_LOCATION}/lib/liblog4cplus.so
            ${EXTERNAL_INSTALL_LOCATION}/lib/liblog4cplus.dylib
)
set(LOG4CPLUS_LIBRARIES ${EXTERNAL_INSTALL_LOCATION}/lib/liblog4cplus.so)
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(LOG4CPLUS_LIBRARIES ${EXTERNAL_INSTALL_LOCATION}/lib/liblog4cplus.dylib)
endif()

# ---------------------------------------------------------------------------
# One code base for both deployment models. The model (client-server or
# peer-to-peer) is selected at run time: dagonfs --model ..., the
# dagonfs_model key of DAGonFS.ini, or dagonfs::VolumeOptions in the C++ API.
# ---------------------------------------------------------------------------

# Option parsing has no MPI/FUSE dependency and is shared with the launcher.
add_library(dagonfs_options STATIC src/volume/Options.cpp src/volume/Options.hpp)
target_include_directories(dagonfs_options PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/volume)
set_target_properties(dagonfs_options PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(dagonfs_core STATIC
        src/volume/Volume.cpp
        src/ramfs/FileSystem.cpp
        src/nodes/Nodes.cpp
        src/nodes/INode.cpp
        src/nodes/File.cpp
        src/nodes/Directory.cpp
        src/nodes/SpecialINode.cpp
        src/nodes/SymbolicLink.cpp
        src/blocks/Blocks.cpp
        src/blocks/DataBlock.cpp
        src/mpi/DataBlockManager.cpp
        src/mpi/DistributedCode.cpp
        src/mpi/RequestSender.cpp
        src/locks/LockTable.cpp
        src/utils/ArgumentParser.cpp
        src/frontends/native/NativeFrontend.cpp
        src/frontends/native/Client.c
)
target_compile_definitions(dagonfs_core PUBLIC
        FUSE_USE_VERSION=32 _FILE_OFFSET_BITS=64
        # The C API is used; skip the deprecated MPI C++ bindings.
        OMPI_SKIP_MPICXX=1 MPICH_SKIP_MPICXX=1)
if(APPLE)
    # macFUSE defaults to Darwin-specific callback and reply structs. The
    # filesystem uses the portable libfuse3 struct stat API throughout.
    target_compile_definitions(dagonfs_core PUBLIC FUSE_DARWIN_ENABLE_EXTENSIONS=0)
endif()
target_link_libraries(dagonfs_core PUBLIC dagonfs_options ${FUSE3_LIBRARIES} ${MPI_C_LIBRARIES} ${LOG4CPLUS_LIBRARIES} -lpthread)
set_target_properties(dagonfs_core PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(dagonfs_preload SHARED src/frontends/preload/dagonfs_preload.c src/frontends/native/Client.c)
target_include_directories(dagonfs_preload PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/frontends/native)
target_link_libraries(dagonfs_preload PRIVATE -lpthread ${CMAKE_DL_LIBS})
set_target_properties(dagonfs_preload PROPERTIES OUTPUT_NAME dagonfs_preload)
# The sources include log4cplus headers produced by the external project, so
# they must not be compiled before it has been installed in the build tree.
add_dependencies(dagonfs_core log4cplus)

add_executable(dagonfs src/main.cpp)
target_link_libraries(dagonfs PRIVATE dagonfs_core)

add_executable("${PROJECT_NAME}_Launcher" src/DAGonFS_Launcher.cpp)
target_link_libraries("${PROJECT_NAME}_Launcher" PRIVATE dagonfs_options)

# Backward-compatible names: each alias defaults to its model.
foreach(alias ${PROJECT_NAME}_CS.exe ${PROJECT_NAME}_P2P.exe)
    add_custom_command(TARGET dagonfs POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E create_symlink dagonfs ${alias}
            WORKING_DIRECTORY $<TARGET_FILE_DIR:dagonfs>
            VERBATIM)
endforeach()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(dagonfs_core PRIVATE -Wall -Wextra)
    target_compile_options(dagonfs_options PRIVATE -Wall -Wextra)
    target_compile_options(dagonfs PRIVATE -Wall -Wextra)
    target_compile_options(${PROJECT_NAME}_Launcher PRIVATE -Wall -Wextra)
endif()

# Installation: the bundled log4cplus shared library is installed next to the
# executables' library directory and found through a relative RPATH.
if(APPLE)
    set(DAGONFS_INSTALL_RPATH "@loader_path/../lib")
else()
    set(DAGONFS_INSTALL_RPATH "$ORIGIN/../lib")
endif()
set_target_properties(dagonfs PROPERTIES INSTALL_RPATH "${DAGONFS_INSTALL_RPATH}")
install(TARGETS dagonfs ${PROJECT_NAME}_Launcher dagonfs_preload DESTINATION bin)
install(TARGETS dagonfs_core dagonfs_options ARCHIVE DESTINATION lib)
install(FILES src/volume/Options.hpp src/volume/Volume.hpp DESTINATION include/dagonfs)
install(DIRECTORY ${EXTERNAL_INSTALL_LOCATION}/lib/ DESTINATION lib FILES_MATCHING PATTERN "liblog4cplus*")
install(FILES src/DAGonFS.ini DESTINATION "${SHAREDIR}")
install(CODE "
    foreach(alias ${PROJECT_NAME}_CS.exe ${PROJECT_NAME}_P2P.exe)
        execute_process(COMMAND \${CMAKE_COMMAND} -E create_symlink dagonfs \${alias}
                        WORKING_DIRECTORY \$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/bin)
    endforeach()
")

if(BUILD_TESTING)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    add_test(
        NAME dagonfs-source-contract
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_source_contract.py
    )
    add_test(
        NAME dagonfs-usage
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/usage.py ${CMAKE_CURRENT_BINARY_DIR}
    )
    # The lock table is pure bookkeeping: unit-tested without MPI or FUSE.
    add_executable(test_lock_table tests/test_lock_table.cpp src/locks/LockTable.cpp)
    add_test(NAME dagonfs-lock-table COMMAND test_lock_table)
    add_executable(test_frontend_options tests/test_frontend_options.cpp)
    target_link_libraries(test_frontend_options PRIVATE dagonfs_options)
    add_test(NAME dagonfs-frontend-options COMMAND test_frontend_options)
    # Parallel MPI-IO client for the mounted tests (tests/mount.py runs it as
    # an MPI job of its own against the mounted volume).
    add_executable(dagonfs_mpiio_check tests/mpiio_check.c)
    target_include_directories(dagonfs_mpiio_check SYSTEM PRIVATE ${MPI_C_INCLUDE_DIRS})
    target_link_libraries(dagonfs_mpiio_check PRIVATE ${MPI_C_LIBRARIES})
    add_test(
        NAME dagonfs-launcher-dry-run
        COMMAND ${PROJECT_NAME}_Launcher ${CMAKE_CURRENT_SOURCE_DIR}/src/DAGonFS.ini --dry-run
    )
endif()
