#pragma once

#include "utils.h"
#include <memory>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

class Coordinator {
public:
  Coordinator();
  ~Coordinator();

  void start_rpc_server();

  // rpc调用
  bool set_erasure_coding_parameters(EC_schema ec_schema);
  std::pair<std::string, int> get_proxy_location(std::string key,
                                                 size_t value_len);

  std::string echo(std::string s);

private:
  void generate_placement_plan(std::vector<unsigned int> &nodes, unsigned int stripe_id);

  std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_{nullptr};
  EC_schema ec_schema_;
  std::unordered_map<int, stripe_item> stripe_info_;
  unsigned int next_stripe_id_{0};
};