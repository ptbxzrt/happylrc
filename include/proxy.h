#include "utils.h"
#include <ylt/coro_rpc/coro_rpc_server.hpp>

class Proxy {
public:
  Proxy(std::string ip, int port);
  ~Proxy();

  // rpc调用
  void start_encode_and_store_object(placement_info placement);
  void decode_and_transfer_data(placement_info placement);

private:
  std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_{nullptr};
  int port_for_rpc_;
  int port_for_transfer_data_;
  std::string ip_;
  int port_;
};