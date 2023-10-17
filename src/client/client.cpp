#include "../../include/client.h"

Client::Client() {
  rpc_client_ = std::make_unique<coro_rpc::coro_rpc_client>();
}

void Client::connect_to_coordinator() {
  async_simple::coro::syncAwait(
      rpc_client_->connect("localhost", std::to_string(COORDINATOR_RPC_PORT)));
}

void Client::set_ec_parameter(EC_schema ec_schema) {
  auto r = async_simple::coro::syncAwait(
      rpc_client_->call<&Coordinator::set_erasure_coding_parameters>(
          ec_schema));
  assert(r.value());
}

void Client::set(std::string key, std::string value) {
  auto [ip, port] = async_simple::coro::syncAwait(
                        rpc_client_->call<&Coordinator::get_proxy_location>(
                            key, value.size()))
                        .value();
}

std::string Client::get(std::string key) {}