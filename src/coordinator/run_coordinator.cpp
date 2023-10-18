#include "../../include/coordinator.h"

int main(int argc, char **argv) {
  Coordinator coordinator("0.0.0.0", COORDINATOR_RPC_PORT);
  coordinator.start_rpc_server();
  return 0;
}