#include "../../include/coordinator.h"

int main(int argc, char **argv) {
  Coordinator coordinator;
  coordinator.start_rpc_server();
  return 0;
}