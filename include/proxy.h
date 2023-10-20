#pragma once

#include "utils.h"
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

class Proxy {
public:
  Proxy(std::string ip, int port);
  ~Proxy();

  void start();

  // rpc调用
  void start_encode_and_store_object(placement_info placement);
  void decode_and_transfer_data(placement_info placement);
  void main_repair(main_repair_plan repair_plan);
  void help_repair(help_repair_plan repair_plan);

private:
  void write_to_datanode(const char *key, size_t key_len, const char *value,
                         size_t value_len, const char *ip, int port);
  void read_from_datanode(const char *key, size_t key_len, char *value,
                          size_t value_len, const char *ip, int port);

  std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_{nullptr};
  int port_for_rpc_;
  int port_for_transfer_data_;
  std::string ip_;
  asio::io_context io_context_{};
  asio::ip::tcp::acceptor acceptor_;
  std::mutex mutex_;
};