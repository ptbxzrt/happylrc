#include "../../include/datanode.h"

int main(int argc, char **argv) {
  my_assert(argc == 3);

  std::string ip = argv[1];
  std::string port = argv[2];
  Datanode datanode(ip, std::stoi(port));
  return 0;
}