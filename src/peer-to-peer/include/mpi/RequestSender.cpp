//
// Created by frank on 1/15/25.
//

#include "RequestSender.hpp"

#include <cstring>
#include <iostream>
#include <mpi.h>

#include "DistributedCode.hpp"
#include "mpi_data.hpp"

using namespace std;

namespace {
template <size_t N>
void copyRequestPath(char (&destination)[N], const string& source) {
    strncpy(destination, source.c_str(), N - 1);
    destination[N - 1] = '\0';
}
}

void RequestSender::sendWriteRequest(int sourceRank, int mpiWorldSize) {
	RequestPacket loopRequest;
	loopRequest.type = WRITE;
	for (int i=0; i<mpiWorldSize; i++) {
		if (i != sourceRank) {
			MPI_Send(&loopRequest, sizeof(RequestPacket), MPI_BYTE, i, 0, MPI_COMM_WORLD);
		}
	}
}

void RequestSender::sendReadRequest(int sourceRank, int mpiWorldSize) {
	RequestPacket loopRequest;
	loopRequest.type = READ;
	for (int i=0; i<mpiWorldSize; i++) {
		if (i != sourceRank) {
			MPI_Send(&loopRequest, sizeof(RequestPacket), MPI_BYTE, i, 0, MPI_COMM_WORLD);
		}
	}
}

void RequestSender::sendCreateFileRequest(string name, int sourceRank, int mpiWorldSize) {
	RequestPacket loopRequest;
	loopRequest.type = CREATE_FILE;
	FileCreationRequest fileCreateRequest;
	copyRequestPath(fileCreateRequest.name, name);
	for (int i=0 ; i<mpiWorldSize ; i++) {
		MPI_Send(&loopRequest, sizeof(RequestPacket), MPI_BYTE, i, 0, MPI_COMM_WORLD);
		if (i != sourceRank) {
			MPI_Send(&fileCreateRequest, sizeof(FileCreationRequest), MPI_BYTE, i, 0, MPI_COMM_WORLD);
		}
	}
}

void RequestSender::sendDeleteFileRequest(string name, int sourceRank, int mpiWorldSize) {
	RequestPacket loopRequest;
	loopRequest.type = DELETE_FILE;
	FileDeletionRequest fileDeleteRequest;
	copyRequestPath(fileDeleteRequest.name, name);
	for (int i=0 ; i<mpiWorldSize ; i++) {
		MPI_Send(&loopRequest, sizeof(RequestPacket), MPI_BYTE, i, 0, MPI_COMM_WORLD);
		if (i != sourceRank) {
			MPI_Send(&fileDeleteRequest, sizeof(FileDeletionRequest), MPI_BYTE, i, 0, MPI_COMM_WORLD);
		}
	}
}

void RequestSender::sendCreateDirectoryRequest(string dirAbsPath, int sourceRank, int mpiWorldSize) {
	RequestPacket loopRequest;
	loopRequest.type = CREATE_DIR;
	DirectoryCreationRequest dirCreateRequest;
	copyRequestPath(dirCreateRequest.absolutePath, dirAbsPath);
	for (int i=0; i< mpiWorldSize; i++) {
		MPI_Send(&loopRequest, sizeof(RequestPacket), MPI_BYTE, i, 0, MPI_COMM_WORLD);
		if (i != sourceRank)
			MPI_Send(&dirCreateRequest, sizeof(DirectoryCreationRequest), MPI_BYTE, i, 0, MPI_COMM_WORLD);
	}
}

void RequestSender::sendDeleteDirectoryRequest(string dirAbsPath, int sourceRank, int mpiWorldSize) {
	RequestPacket loopRequest;
	loopRequest.type = DELETE_DIR;
	DirectoryDeletionRequest dirDeleteRequest;
	copyRequestPath(dirDeleteRequest.absolutePath, dirAbsPath);
	for (int i=0; i< mpiWorldSize; i++) {
		MPI_Send(&loopRequest, sizeof(RequestPacket), MPI_BYTE, i, 0, MPI_COMM_WORLD);
		if (i != sourceRank)
			MPI_Send(&dirDeleteRequest, sizeof(DirectoryDeletionRequest), MPI_BYTE, i, 0, MPI_COMM_WORLD);
	}
}

void RequestSender::sendRenameRequest(std::string oldName, std::string newName, int sourceRank, int mpiWorldSize) {
	RequestPacket loopRequest;
	loopRequest.type = RENAME;
	RenameRequest renameRequest;
	copyRequestPath(renameRequest.oldName, oldName);
	copyRequestPath(renameRequest.newName, newName);
	for (int i=0; i< mpiWorldSize; i++) {
		MPI_Send(&loopRequest, sizeof(RequestPacket), MPI_BYTE, i, 0, MPI_COMM_WORLD);
		if (i != sourceRank)
			MPI_Send(&renameRequest, sizeof(RenameRequest), MPI_BYTE, i, 0, MPI_COMM_WORLD);
	}
}


void RequestSender::sendTerminationRequest(int sourceRank, int mpiWorldSize) {
	cout << "Process " << sourceRank << " - Sending termination request" << endl;
	RequestPacket termationRequest;
	termationRequest.type = TERMINATE;
	for (int i=0; i<mpiWorldSize; i++) {
          MPI_Send(&termationRequest, sizeof(RequestPacket), MPI_BYTE, i, 0, MPI_COMM_WORLD);
	}

}
