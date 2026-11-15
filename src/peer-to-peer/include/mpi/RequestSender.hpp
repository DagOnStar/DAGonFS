//
// Created by frank on 1/15/25.
//

#ifndef REQUESTSENDER_HPP
#define REQUESTSENDER_HPP

#include "../utils/fuse_headers.hpp"
#include <cstdint>
#include <string>

#include "mpi_data.hpp"

/**
 * @brief Originator side of the peer-to-peer control plane.
 *
 * Every send* function delivers one self-describing message to each other peer
 * (sendTerminationRequest also to the sender itself).
 */
class RequestSender {
public:
	/** Header + up to two bounded paths, sent as one message to the selected peers. */
	static void send(const PeerRequest &request, const std::string &first, const std::string &second,
	                 int sourceRank, int mpiWorldSize, bool includeSelf = false);

	static void sendCreateFileRequest(std::string name, uint64_t globalId, int sourceRank, int mpiWorldSize);
	static void sendDeleteFileRequest(std::string name, int sourceRank, int mpiWorldSize);
	static void sendCreateDirectoryRequest(std::string dirAbsPath, int sourceRank, int mpiWorldSize);
	static void sendDeleteDirectoryRequest(std::string dirAbsPath, int sourceRank, int mpiWorldSize);
	static void sendRenameRequest(std::string oldName, std::string newName, int sourceRank, int mpiWorldSize);
	static void sendLinkRequest(std::string targetName, std::string newName, int sourceRank, int mpiWorldSize);
	static void sendTerminationRequest(int sourceRank, int mpiWorldSize);
};

#endif //REQUESTSENDER_HPP
