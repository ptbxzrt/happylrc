#include "../../include/coordinator.h"

int main(int argc, char **argv) {
  // 如果部署在集群上, 一定要将coordinator ip地址设置为1个实际的IP地址,
  // 而非"0.0.0.0", 因为这个IP地址会被proxy使用
  Coordinator coordinator("0.0.0.0", COORDINATOR_RPC_PORT,
                          "/home/ptbxzrt/happylrc/config.xml");
  coordinator.start();
  return 0;
}