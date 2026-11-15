//
// Created by frank on 11/29/24.
//

#include "ArgumentParser.hpp"
#include <cstring>

using namespace std;

ArgumentParser::ArgumentParser(int argc, char** argv) {
	this->copy_args(argc, argv);
}

/* Destructor. Releases the owned copy of the arguments. libfuse duplicates
 * every argument it keeps (fuse_opt_add_arg uses strdup), so the copy can be
 * released once the FUSE session has been destroyed.
 */
ArgumentParser::~ArgumentParser() {
	delete_args(this->argc, this->copied_args);
}

/* Make an owned, NULL-terminated copy of argv[] so that FUSE and MPI never
 * modify the caller's command line.
 * @param argc Number of arguments
 * @param argv The arguments pointer
 * @return The copy of the arguments
 */
char ** ArgumentParser::copy_args(int argc, char * argv[]) {
	delete_args(this->argc, this->copied_args);
	this->argc = argc;
	this->copied_args = new char*[argc + 1];
	for (int i = 0; i < argc; ++i) {
		const size_t len = strlen(argv[i]) + 1;
		copied_args[i] = new char[len];
		memcpy(copied_args[i], argv[i], len);
	}
	copied_args[argc] = nullptr;
	return copied_args;
}

/* Replace one argument of the owned copy.
 */
void ArgumentParser::replace_arg(int index, const char *value) {
	if (copied_args == nullptr || index < 0 || index >= argc || value == nullptr) {
		return;
	}
	const size_t len = strlen(value) + 1;
	char *copy = new char[len];
	memcpy(copy, value, len);
	delete[] copied_args[index];
	copied_args[index] = copy;
}

/* Free the memory previously allocated for the copy of arguments
 * @param argc Number of arguments
 * @param argv The actual data to free
 * */
void ArgumentParser::delete_args(int argc, char** argv){
	if (argv == nullptr) {
		return;
	}
	for (int i = 0; i < argc; i++) {
		delete[] argv[i];
	}
	delete[] argv;
	if (argv == this->copied_args) {
		this->copied_args = nullptr;
		this->argc = 0;
	}
}
