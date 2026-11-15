//
// Created by frank on 1/2/25.
//

#ifndef ARGUMENTPARSER_HPP
#define ARGUMENTPARSER_HPP

/**
 * @brief Owns a mutable, NULL-terminated copy of the command line.
 */
class ArgumentParser {
private:
	int argc;
	char **copied_args;

public:
	ArgumentParser() : argc(0), copied_args(nullptr) {};
	ArgumentParser(int argc, char **argv);
	~ArgumentParser();
	ArgumentParser(const ArgumentParser &) = delete;
	ArgumentParser &operator=(const ArgumentParser &) = delete;
	char **copy_args(int argc, char * argv[]);
	char **getCopiedArgs(){return copied_args;};
	void replace_arg(int index, const char *value);
	void delete_args(int argc, char **argv);
};

#endif //ARGUMENTPARSER_HPP
