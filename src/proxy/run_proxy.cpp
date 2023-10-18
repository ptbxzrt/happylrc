#include "../../include/proxy.h"

int main(int argc, char **argv) {
  my_assert(argc == 5);

  std::string ip = argv[1];
  std::string port = argv[2];
  std::string coordinator_ip = argv[3];
  std::string coordinator_port = argv[4];
  Proxy proxy(ip, std::stoi(port), coordinator_ip, std::stoi(coordinator_port));
  proxy.start();
  return 0;
}