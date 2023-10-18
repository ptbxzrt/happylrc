#include "../../include/proxy.h"

Proxy::Proxy(std::string ip, int port)
    : ip_(ip), port_(port), port_for_rpc_(port + 1),
      port_for_transfer_data_(port) {
  rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(1, port_for_rpc_);
  rpc_server_->register_handler<&Proxy::start_encode_and_store_object>(this);
  rpc_server_->register_handler<&Proxy::decode_and_transfer_data>(this);
}

Proxy::~Proxy() { rpc_server_->stop(); }

void Proxy::start_encode_and_store_object(placement_info placement) {}

void Proxy::decode_and_transfer_data(placement_info placement) {}