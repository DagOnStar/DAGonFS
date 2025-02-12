# DAGonFileSystem
DAGonFileSystem (DAGonFS) is an ad-hoc file system written in C++ that transforms a directory into a distributed user-space and in-RAM storage system. It's main purpose is the improvement of DAGonStar performance.

# Motivation
The DAGonStar workflow engine supports two types of workflows: taskflow and dataflow. The focus is on dataflow because DAGonStar implements a specific independence model called the workflow:// schema. This component evaluates all task dependencies and manages data transfer between tasks when these dependencies involve data. According to this paradigm, outputs are produced as files (or directories), with the scratch directory as the root. DAGonStar's design treats a workflow:// schema as the root of the current workflow, acting as a virtual file system. In this context, workflow://<unique_task_name>/ represents the root of the scratch directory created by the DAGonStar Runtime. In a high-performance computing environment, it is crucial to reduce execution time caused by I/O operations on persistent storage (such as file and directory creation). Developing a file system that uses RAM as a temporary storage device would help reduce execution time and improve DAGonStar's performance.

# Features
* Trasparency with client proccesses
* Two models of execution:
  * Client-Server model, for execution on a single computational node
  * Peer-to-Peer model, for execution on multiple computational node on hpc clusters
* Possibility to save the file system state to persistent storage

# Acknowledgments
DAGonFileSystem is part of the research project funded through a competitive procedure under the PNRR cascade call, issued by the National Center for High-Performance Computing, Spoke 1: "Directed Acyclic Graph on Cross-Application Programmable I/O" (DAGonCAPIO, https://dagoncapio.uniparthenope.it, CUP J33C22001170001) PNRR M4C2 I1.4 National Champion ICSC – Cascade call SPOKE 1 – “Future HPC and Big Data”.
Thematic Area: 3 – Development and testing of methodologies, middleware, and tools for distributed computing for cloud-HPC, AI-HPC, AI-cloud-HPC convergence, and their use in interdisciplinary applications.

# Performance
The results obtained from file copy operations were excellent, as it was possible to obtain an improvement in execution time up to 47% compared to the execution time of the same operations on a Shared Network File System (NFS). Moreover, using DAGonFS during the execution of a dataflow with DAGonStar allows for a 37% improvement in execution time compared to NFS.

# References
* De Vita Ciro Giuseppe, Caramiello Dario, Mellone Gennaro, Sánchez-Gallegos, Dante Domizzi, Mele Valeria, Cavallo Stefania, Di Luccio Diana.
An ad-hoc file system accelerated workflow application for accidental fire fast response.
In: Proceedings of the 2nd Workshop on Workflows in Distributed Environments. WiDE ’24. Athens, Greece: Association for Computing Machinery, 2024
[link](https://dl.acm.org/doi/10.1145/3642978.3652836)

# Requirements
DAGonFileSystem was implemented using the OpenMPI (aviable at: https://github.com/open-mpi/ompi) and libfuse (available at: https://github.com/libfuse/libfuse https://osxfuse.github.io) (libfuse3-3.2 or later) libraries.
The use of OpenMPI was crucial to making it a distributed system, enabling message exchanges between processes to distribute and collect data.
Meanwhile, libfuse allowed for the interception of system calls to execute the file system functionalities implemented in DAGonFS. 
Therefore, to be successfully compiled, the installation of OpenMPI 4.1.0 (or higher) and libfuse libfuse3-3.2 (or higher) is required.
DAGonFileSystem builds with CMake version 3.0 or greater.

# Installation
```bash
git clone https://github.com/DagOnStar/DAGonFS.git
cd DAGonFS
mkdir build
cd build
cmake -DUSE_MPI=ON ../
make
```
# Demo
DAGonFileSystem uses a launcher that read from a config file named "DAGonFS.ini" the values of the paramteres that would be given as input to DAGonFS executables. To run DAGonFileSystem make sure to have set the configuration file 'DAGonFS.ini' before launching and also to have copied it and the two DAGonFS executables in the same directory of the launcher.

```bash
cd DAGonFS
cd build
cp ../src/DAGonFS.ini ./
./DAGonFS_Launcher
```
