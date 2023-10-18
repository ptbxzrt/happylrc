#include "../../include/client.h"

Client::Client(std::string ip, int port, std::string coordinator_ip,
               int coordinator_port)
    : ip_(ip), port_for_transfer_data_(port), coordinator_ip_(coordinator_ip),
      coordinator_port_(coordinator_port),
      acceptor_(io_context_, asio::ip::tcp::endpoint(
                                 asio::ip::address::from_string(ip.c_str()),
                                 port_for_transfer_data_)) {
  rpc_client_ = std::make_unique<coro_rpc::coro_rpc_client>();
}

void Client::connect_to_coordinator() {
  async_simple::coro::syncAwait(
      rpc_client_->connect(coordinator_ip_, std::to_string(coordinator_port_)));
}

void Client::set_ec_parameter(EC_schema ec_schema) {
  auto r = async_simple::coro::syncAwait(
      rpc_client_->call<&Coordinator::set_erasure_coding_parameters>(
          ec_schema));
  my_assert(r.value());
}

void Client::set(std::string key, std::string value) {
  auto [proxy_ip, proxy_port] =
      async_simple::coro::syncAwait(
          rpc_client_->call<&Coordinator::get_proxy_location>(key,
                                                              value.size()))
          .value();

  try {
    asio::ip::tcp::socket socket(io_context_);
    asio::ip::tcp::endpoint endpoint(asio::ip::make_address(proxy_ip),
                                     proxy_port);
    socket.connect(endpoint);

    asio::write(socket, asio::buffer(key, key.size()));
    asio::write(socket, asio::buffer(value, value.size())); // 发送数据

    socket.shutdown(asio::ip::tcp::socket::shutdown_both);
    socket.close();
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }

  auto commited = async_simple::coro::syncAwait(
                      rpc_client_->call<&Coordinator::check_commit>(key))
                      .value();
  my_assert(commited);
}

std::string Client::get(std::string key) {
  size_t value_len = async_simple::coro::syncAwait(
                         rpc_client_->call<&Coordinator::ask_for_data>(
                             key, ip_, port_for_transfer_data_))
                         .value();

  asio::ip::tcp::socket peer(io_context_);
  acceptor_.accept(peer);

  std::string key_buf(0, key.size());
  std::string value_buf(0, value_len);

  size_t readed_len_of_key =
      asio::read(peer, asio::buffer(key_buf.data(), key_buf.size()));
  my_assert(readed_len_of_key == key.size() && key_buf == key);

  size_t readed_len_of_value =
      asio::read(peer, asio::buffer(value_buf.data(), value_buf.size()));
  my_assert(readed_len_of_value == value_len);

  peer.shutdown(asio::ip::tcp::socket::shutdown_both);
  peer.close();

  return value_buf;
}