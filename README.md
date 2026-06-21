# DAGonFileSystem
DAGonFileSystem (DAGonFS) is an in-memory, MPI-distributed FUSE filesystem written in C++ for improving I/O performance in DAGonStar workflows. It provides client-server and peer-to-peer deployment models and is designed for temporary workflow data rather than durable storage.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Configuration and operation](docs/CONFIGURATION.md)
- [POSIX/FUSE compatibility](docs/POSIX-COMPATIBILITY.md)
- [Contributor guide](AGENTS.md)

# Abstract
The following repository presents the development and implementation of DAGonFileSystem (DAGonFS), an ad-hoc file system that transforms a directory into a distributed user-space and in-RAM storage system. The results obtained from file copy operations were excellent, as it was possible to obtain an improvement in execution time up to 47% compared to the execution time of the same operations on a Shared Network File System (NFS).

# Motivation
The DAGonStar workflow engine supports two types of workflows: taskflow and dataflow. The focus is on dataflow because DAGonStar implements a specific independence model called the workflow:// schema. This component evaluates all task dependencies and manages data transfer between tasks when these dependencies involve data. According to this paradigm, outputs are produced as files (or directories), with the scratch directory as the root. DAGonStar's design treats a workflow:// schema as the root of the current workflow, acting as a virtual file system. In this context, workflow://<unique_task_name>/ represents the root of the scratch directory created by the DAGonStar Runtime. In a high-performance computing environment, it is crucial to reduce execution time caused by I/O operations on persistent storage (such as file and directory creation). Developing a file system that uses RAM as a temporary storage device would help reduce execution time and improve DAGonStar's performance.
![Motivation](/figures/MotivationDAGonFS.png)

# Architecture
DAGonFileSystem has been implemented in two versions, following two distinct models: Client-Server and Peer-to-Peer.

## Client-Server model
The Client-Server model involves the execution of two types of processes: a client process, also called the master process, which originates the requests for file system operations, and one or more server processes that will remain waiting for a request from the master process. Once a server process receives a request, it is served. The following image shows the modules, their interactions, and the data structures that are part of DAGonFS Client-Server.
![Client-Server model](/figures/CSArchitecture.jpg)

The actual data of the file in DAGonFS are stored in the DataBlock data strucutre.

## Peer-to-Peer model
The Peer-to-Peer model involves the execution of a generic peer process on all computational nodes where DAGonFS is running. In this model, unlike the previous one, all peers share the same responsibility, which is to wait for an operation originated by any of the peers. Once a request is received, all other peers respond to that request.
The following figure shows the DAGonFS Peer-to-Peer modules, their interactions, and the data structures.
![Peer-to-Peer model](/figures/P2PArchitecture.jpg)

The actual data of the file in DAGonFS are stored in the DataBlock data strucutre.

# Features
* Trasparency with client proccesses
* Two type of executable versions:
  * Client-Server, for execution on a single computational node
  * Peer-to-Peer, for execution on multiple computational node on hpc clusters
* Possibility to save the file system state to persistent storage

# Acknowledgments
DAGonFileSystem is part of the research project funded through a competitive procedure under the PNRR cascade call, issued by the National Center for High-Performance Computing, Spoke 1: "Directed Acyclic Graph on Cross-Application Programmable I/O" (DAGonCAPIO, https://dagoncapio.uniparthenope.it, CUP J33C22001170001) PNRR M4C2 I1.4 National Champion ICSC – Cascade call SPOKE 1 – “Future HPC and Big Data”.
Thematic Area: 3 – Development and testing of methodologies, middleware, and tools for distributed computing for cloud-HPC, AI-HPC, AI-cloud-HPC convergence, and their use in interdisciplinary applications.

# Performance
The results obtained from file copy operations were excellent, as it was possible to obtain an improvement in execution time up to 47% compared to the execution time of the same operations on a Shared Network File System (NFS). Moreover, using DAGonFS during the execution of a dataflow with DAGonStar allows for a 37% improvement in execution time compared to NFS.
The following plots show the execution time of the file copy operation execute on an NFS and with two model of DAGonFileSystem.
![Execution time as the number of process varies](/figures/DAGonFS_P2P-processes.png)
![Execution time as the data block size varies](/figures/DAGonFS_P2P-blocksize.png)

Indeed, the following plots show the execution time of a dataflow with DAGonStar using DAGonFS.
![DAGonStar execution as the number of process varies](/figures/DAGonFS_with_DAGonStar-processes.png)
![DAGonStar execution as the data block size](/figures/DAGonFS_with_DAGonStar-blocksize.png)

# References
* De Vita Ciro Giuseppe, Caramiello Dario, Mellone Gennaro, Sánchez-Gallegos, Dante Domizzi, Mele Valeria, Cavallo Stefania, Di Luccio Diana.
An ad-hoc file system accelerated workflow application for accidental fire fast response.
In: Proceedings of the 2nd Workshop on Workflows in Distributed Environments. WiDE ’24. Athens, Greece: Association for Computing Machinery, 2024
[link](https://dl.acm.org/doi/10.1145/3642978.3652836)

# Requirements
DAGonFileSystem was implemented using the OpenMPI and libfuse libraries.
The use of OpenMPI was crucial to making it a distributed system, enabling message exchanges between processes to distribute and collect data.
Meanwhile, libfuse allowed for the interception of system calls to execute the file system functionalities implemented in DAGonFS. 
Therefore, to be successfully compiled, the installation of OpenMPI 4.1.0 (or higher) and libfuse libfuse3-3.2 (or higher) is required.
DAGonFileSystem requires CMake 3.20 or newer and a compiler with C++23 support.

# Installation
Clone the repository and enter in the repository directory
```bash
git clone https://github.com/DagOnStar/DAGonFS.git
cd DAGonFS
```
Configure the building enviroment
```bash
cmake -S . -B build -DUSE_MPI=ON
```
Make DAGonFileSystem
```bash
cmake --build build
```

# Demo
In both models, when DAGonFS is launched, the path of a directory must be provided, which will serve as the mountpoint directory for DAGonFS. Once running, operations on DAGonFS can be performed on the directory just like any other directory in a common file system.
The difference between the two models lies in where the operations on the mountpoint directory are executed: in the Client-Server model, operations can only be executed on the computational node where the master process is running, as it is the only one intercepting the system calls. In the Peer-to-Peer model, each computational node has its own mountpoint directory, which is considered an entry point for DAGonFS, and operations on DAGonFS can be executed on any of the computational nodes.
DAGonFileSystem uses a launcher that reads `DAGonFS.ini` and starts the selected model through MPI. Configure the file and copy it, the launcher, and both executables into the same execution directory. See [Configuration and operation](docs/CONFIGURATION.md) for the configuration reference and shutdown procedure.

To run DAGonFileSystem, you need to assign values to the configuration variables within a "DAGonFS.ini" file.
Once assigned, it is recommended to create your own folder inside which you should copy the newly configured "DAGonFS.ini" file.
```bash
mkdir dagonfs_exec
cp DAGonFS/src/DAGonFS.ini dagonfs_exec
```
Next, you need to copy the 3 executables found in the output folder of the DAGonFS installation into the newly created folder.
```bash
cp DAGonFS/build/DAGonFS_CS.exe dagonfs_exec
cp DAGonFS/build/DAGonFS_P2P.exe dagonfs_exec
cp DAGonFS/build/DAGonFS_Launcher dagonfs_exec
```
Finally, change the directory to the created one and run the DAGonFS launcher.
```bash
cd dagonfs_exec
./DAGonFS_Launcher
```
To terminate the execution of DAGonFS it's necessary to unmount it, an example may be the ```fusermount3``` command with ```-u``` option followed by the given path for the mountpoint.

# Open-Source external projects
* [in-RAM file system](https://github.com/watkipet/fuse-cpp-ramfs) an open source project that implement an in-RAM file system
* [libFUSE3](https://github.com/libfuse/libfuse) an interface for userspace programs to export a filesystem to the Linux kernel
* [log4cplus](https://github.com/log4cplus/log4cplus) a simple to use C++23 logging API providing thread--safe, flexible, and arbitrarily granular control over log management and configuration
