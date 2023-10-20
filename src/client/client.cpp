#include "../../include/client.h"

Client::Client(std::string ip, int port, std::string coordinator_ip,
               int coordinator_port)
    : ip_(ip), port_for_transfer_data_(port), coordinator_ip_(coordinator_ip),
      coordinator_port_(coordinator_port),
      acceptor_(io_context_, asio::ip::tcp::endpoint(
                                 asio::ip::address::from_string(ip.c_str()),
                                 port_for_transfer_data_)) {
  rpc_coordinator_ = std::make_unique<coro_rpc::coro_rpc_client>();
  async_simple::coro::syncAwait(rpc_coordinator_->connect(
      coordinator_ip_, std::to_string(coordinator_port_)));
}

Client::~Client() { acceptor_.close(); }

void Client::set_ec_parameter(EC_schema ec_schema) {
  async_simple::coro::syncAwait(
      rpc_coordinator_->call<&Coordinator::set_erasure_coding_parameters>(
          ec_schema));
}

void Client::set(std::string key, std::string value) {
  auto [proxy_ip, proxy_port] =
      async_simple::coro::syncAwait(
          rpc_coordinator_->call<&Coordinator::get_proxy_location>(
              key, value.size()))
          .value();

  asio::ip::tcp::socket peer(io_context_);
  asio::ip::tcp::endpoint endpoint(asio::ip::make_address(proxy_ip),
                                   proxy_port);
  peer.connect(endpoint);

  asio::write(peer, asio::buffer(key, key.size()));
  asio::write(peer, asio::buffer(value, value.size()));

  std::vector<char> finish(1);
  asio::read(peer, asio::buffer(finish, finish.size()));

  asio::error_code ignore_ec;
  peer.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
  peer.close(ignore_ec);

  async_simple::coro::syncAwait(
      rpc_coordinator_->call<&Coordinator::commit_object>(key));
}

std::string Client::get(std::string key) {
  size_t value_len = async_simple::coro::syncAwait(
                         rpc_coordinator_->call<&Coordinator::ask_for_data>(
                             key, ip_, port_for_transfer_data_))
                         .value();

  asio::ip::tcp::socket peer(io_context_);
  acceptor_.accept(peer);

  std::string key_buf(key.size(), 0);
  std::string value_buf(value_len, 0);
  my_assert(key_buf.size() == key.size());
  my_assert(value_buf.size() == value_len);

  size_t readed_len_of_key =
      asio::read(peer, asio::buffer(key_buf.data(), key_buf.size()));
  my_assert(readed_len_of_key == key.size() && key_buf == key);

  size_t readed_len_of_value =
      asio::read(peer, asio::buffer(value_buf.data(), value_buf.size()));
  my_assert(readed_len_of_value == value_len);

  asio::error_code ignore_ec;
  peer.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
  peer.close(ignore_ec);

  return value_buf;
}

void Client::repair(std::vector<unsigned int> failed_node_ids) {
  // 以node为单位修复
  // 只实现了single node repair
  // 当然可以扩充实现multi node repair
  my_assert(failed_node_ids.size() == 1);

  async_simple::coro::syncAwait(
      rpc_coordinator_->call<&Coordinator::ask_for_repair>(failed_node_ids));
}