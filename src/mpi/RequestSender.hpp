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
 * @brief Originator side of the control plane.
 *
 * Every send* function delivers one self-describing message to each other rank
 * (sendTerminationRequest optionally also to the sender's own listener).
 */
class RequestSender {
public:
	/** Header + up to two bounded paths and a bounded value, sent as one message to the selected peers. */
	static void send(const PeerRequest &request, const std::string &first, const std::string &second,
	                 int sourceRank, int mpiWorldSize, bool includeSelf = false,
	                 const std::string &value = std::string());

	static void sendCreateFileRequest(std::string name, uint64_t globalId, int sourceRank, int mpiWorldSize);
	static void sendDeleteFileRequest(std::string name, int sourceRank, int mpiWorldSize);
	static void sendCreateDirectoryRequest(std::string dirAbsPath, int sourceRank, int mpiWorldSize);
	static void sendDeleteDirectoryRequest(std::string dirAbsPath, int sourceRank, int mpiWorldSize);
	static void sendRenameRequest(std::string oldName, std::string newName, int sourceRank, int mpiWorldSize);
	static void sendLinkRequest(std::string targetName, std::string newName, int sourceRank, int mpiWorldSize);
	static void sendSymlinkRequest(std::string target, std::string newName, int sourceRank, int mpiWorldSize);
	static void sendSetAttrRequest(std::string name, const MetadataPacket &meta, int sourceRank, int mpiWorldSize);
	static void sendSetXAttrRequest(std::string name, std::string attribute, std::string value, int flags,
	                                int sourceRank, int mpiWorldSize);
	static void sendRemoveXAttrRequest(std::string name, std::string attribute, int sourceRank, int mpiWorldSize);
	/** Client-server: storage ranks release their slices of the file with data key @p key. */
	static void sendDeleteRequest(uint64_t key, int sourceRank, int mpiWorldSize);
	static void sendTerminationRequest(int sourceRank, int mpiWorldSize, bool includeSelf);
};

#endif //REQUESTSENDER_HPP
