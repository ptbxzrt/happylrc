#pragma once

#include "coordinator.h"
#include "utils.h"
#include <memory>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

class Client {
public:
  Client();

  std::unique_ptr<coro_rpc::coro_rpc_client> &get_rpc_client();
  void connect_to_coordinator();
  void set_ec_parameter(EC_schema ec_schema);

  void set(std::string key, std::string value);
  std::string get(std::string key);

private:
  std::unique_ptr<coro_rpc::coro_rpc_client> rpc_client_{nullptr};
};