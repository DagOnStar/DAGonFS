//
// Created by frank on 1/15/25.
//

#include "RequestSender.hpp"

#include <cstring>
#include <iostream>
#include <vector>
#include <mpi.h>

#include "mpi_data.hpp"

using namespace std;

namespace {

PeerRequest makeRequest(RequestType type) {
	PeerRequest request{};
	request.type = type;
	return request;
}

uint32_t boundedLength(const string &path) {
	// Callers reject longer paths with ENAMETOOLONG before sending; this is a
	// last line of defence that keeps the message well-formed.
	return static_cast<uint32_t>(min(path.size(), DAGONFS_MAX_REQUEST_PATH - 1));
}

} // namespace

void RequestSender::send(const PeerRequest &request, const string &first, const string &second,
                         int sourceRank, int mpiWorldSize, bool includeSelf) {
	PeerRequest header = request;
	header.firstPathLength = boundedLength(first);
	header.secondPathLength = boundedLength(second);

	vector<char> message(sizeof(PeerRequest) + header.firstPathLength + header.secondPathLength);
	memcpy(message.data(), &header, sizeof(PeerRequest));
	memcpy(message.data() + sizeof(PeerRequest), first.data(), header.firstPathLength);
	memcpy(message.data() + sizeof(PeerRequest) + header.firstPathLength, second.data(), header.secondPathLength);

	for (int i = 0; i < mpiWorldSize; i++) {
		if (i != sourceRank || includeSelf) {
			MPI_Send(message.data(), static_cast<int>(message.size()), MPI_BYTE, i, DAGONFS_TAG_REQUEST, MPI_COMM_WORLD);
		}
	}
}

void RequestSender::sendCreateFileRequest(string name, uint64_t globalId, int sourceRank, int mpiWorldSize) {
	PeerRequest request = makeRequest(CREATE_FILE);
	request.globalId = globalId;
	send(request, name, string(), sourceRank, mpiWorldSize);
}

void RequestSender::sendDeleteFileRequest(string name, int sourceRank, int mpiWorldSize) {
	send(makeRequest(DELETE_FILE), name, string(), sourceRank, mpiWorldSize);
}

void RequestSender::sendCreateDirectoryRequest(string dirAbsPath, int sourceRank, int mpiWorldSize) {
	send(makeRequest(CREATE_DIR), dirAbsPath, string(), sourceRank, mpiWorldSize);
}

void RequestSender::sendDeleteDirectoryRequest(string dirAbsPath, int sourceRank, int mpiWorldSize) {
	send(makeRequest(DELETE_DIR), dirAbsPath, string(), sourceRank, mpiWorldSize);
}

void RequestSender::sendRenameRequest(string oldName, string newName, int sourceRank, int mpiWorldSize) {
	send(makeRequest(RENAME), oldName, newName, sourceRank, mpiWorldSize);
}

void RequestSender::sendLinkRequest(string targetName, string newName, int sourceRank, int mpiWorldSize) {
	send(makeRequest(LINK), targetName, newName, sourceRank, mpiWorldSize);
}

void RequestSender::sendDeleteRequest(uint64_t key, int sourceRank, int mpiWorldSize) {
	PeerRequest request = makeRequest(DELETE);
	request.io.inode = static_cast<fuse_ino_t>(key);
	send(request, string(), string(), sourceRank, mpiWorldSize);
}

void RequestSender::sendTerminationRequest(int sourceRank, int mpiWorldSize, bool includeSelf) {
	cout << "Process " << sourceRank << " - Sending termination request" << endl;
	send(makeRequest(TERMINATE), string(), string(), sourceRank, mpiWorldSize, includeSelf);
}
