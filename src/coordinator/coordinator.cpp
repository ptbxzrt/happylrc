#include "../../include/coordinator.h"

std::string Coordinator::echo(std::string s) { return s + "zhaohao"; }

Coordinator::Coordinator() {
  rpc_server_ =
      std::make_unique<coro_rpc::coro_rpc_server>(1, COORDINATOR_RPC_PORT);
  rpc_server_->register_handler<&Coordinator::set_erasure_coding_parameters>(
      this);
  rpc_server_->register_handler<&Coordinator::echo>(this);
}

Coordinator::~Coordinator() { rpc_server_->stop(); }

void Coordinator::start_rpc_server() { auto err_code = rpc_server_->start(); }

bool Coordinator::set_erasure_coding_parameters(EC_schema ec_schema) {
  ec_schema_ = ec_schema;
  return true;
}

std::pair<std::string, int> Coordinator::get_proxy_location(std::string key,
                                                            size_t value_len) {
  meta_info_of_data new_object;
  new_object.encode_type = ec_schema_.encode_type;
  new_object.placement_type = ec_schema_.placement_type;
  new_object.k = ec_schema_.k;
  new_object.real_l = ec_schema_.real_l;
  new_object.g = ec_schema_.g;
  new_object.b = ec_schema_.b;
  new_object.value_len = value_len;

  if (ec_schema_.strip_size_upper >= new_object.value_len) {
    int block_size = std::ceil(static_cast<double>(new_object.value_len) /
                               static_cast<double>(new_object.k));
    block_size = 64 * std::ceil(static_cast<double>(block_size) / 64.0);

    stripe_item temp;
    temp.stripe_id = next_stripe_id_++;
    stripe_info_[temp.stripe_id] = temp;
    stripe_item &stripe = stripe_info_[temp.stripe_id];
    stripe.encode_type = ec_schema_.encode_type;
    stripe.placement_type = ec_schema_.placement_type;
    stripe.k = ec_schema_.k;
    stripe.real_l = ec_schema_.real_l;
    stripe.g = ec_schema_.g;
    stripe.b = ec_schema_.b;
    stripe.value_len = value_len;
    stripe.block_size = block_size;

    generate_placement_plan(stripe.nodes, stripe.stripe_id);
    new_object.stripes.push_back(stripe.stripe_id);

    placement_info placement;
    placement.encode_type = stripe.encode_type;
    placement.stripe_ids.push_back(stripe.stripe_id);
    placement.key = key;
    placement.value_len = stripe.value_len;
    placement.k = stripe.k;
    placement.g = stripe.g;
    placement.real_l = stripe.real_l;
    placement.b = stripe.b;
    placement.block_size = stripe.block_size;
    placement.tail_block_size = -1;

  } else {
  }
}

void Coordinator::generate_placement_plan(std::vector<unsigned int> &nodes,
                                          unsigned int stripe_id) {}