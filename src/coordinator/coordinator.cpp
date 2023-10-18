#include "../../include/coordinator.h"
#include "../../include/tinyxml2.h"

std::string Coordinator::echo(std::string s) { return s + "zhaohao"; }

Coordinator::Coordinator(std::string ip, int port, std::string config_file_path)
    : ip_(ip), port_(port), config_file_path_(config_file_path) {
  rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(1, port_);
  rpc_server_->register_handler<&Coordinator::set_erasure_coding_parameters>(
      this);
  rpc_server_->register_handler<&Coordinator::check_commit>(this);
  rpc_server_->register_handler<&Coordinator::ask_for_data>(this);
  rpc_server_->register_handler<&Coordinator::echo>(this);
  rpc_server_->register_handler<&Coordinator::commit_object>(this);

  init_cluster_info();
  init_proxy_info();
}

Coordinator::~Coordinator() { rpc_server_->stop(); }

void Coordinator::start() { auto err = rpc_server_->start(); }

void Coordinator::init_cluster_info() {
  tinyxml2::XMLDocument xml;
  xml.LoadFile(config_file_path_.c_str());
  tinyxml2::XMLElement *root = xml.RootElement();
  unsigned int node_id = 0;

  for (tinyxml2::XMLElement *cluster = root->FirstChildElement();
       cluster != nullptr; cluster = cluster->NextSiblingElement()) {
    unsigned int cluster_id(std::stoi(cluster->Attribute("id")));
    std::string proxy(cluster->Attribute("proxy"));

    cluster_info_[cluster_id].cluster_id = cluster_id;
    auto pos = proxy.find(':');
    cluster_info_[cluster_id].proxy_ip = proxy.substr(0, pos);
    cluster_info_[cluster_id].proxy_port =
        std::stoi(proxy.substr(pos + 1, proxy.size()));

    for (tinyxml2::XMLElement *node =
             cluster->FirstChildElement()->FirstChildElement();
         node != nullptr; node = node->NextSiblingElement()) {
      cluster_info_[cluster_id].nodes.push_back(node_id);

      std::string node_uri(node->Attribute("uri"));
      node_info_[node_id].node_id = node_id;
      auto pos = node_uri.find(':');
      node_info_[node_id].ip = node_uri.substr(0, pos);
      node_info_[node_id].port =
          std::stoi(node_uri.substr(pos + 1, node_uri.size()));
      node_info_[node_id].cluster_id = cluster_id;
      node_id++;
    }
  }
}

void Coordinator::init_proxy_info() {
  for (auto cur = cluster_info_.begin(); cur != cluster_info_.end(); cur++) {
    connect_to_proxy(cur->second.proxy_ip, cur->second.proxy_port);
  }
}

bool Coordinator::set_erasure_coding_parameters(EC_schema ec_schema) {
  ec_schema_ = ec_schema;
  return true;
}

std::pair<std::string, int> Coordinator::get_proxy_location(std::string key,
                                                            size_t value_len) {
  mutex_.lock();
  if (commited_object_info_.contains(key)) {
    mutex_.unlock();
    my_assert(false);
  }
  mutex_.unlock();

  meta_info_of_object new_object;
  new_object.value_len = value_len;

  placement_info placement;

  if (ec_schema_.strip_size_upper >= new_object.value_len) {
    size_t block_size = std::ceil(static_cast<double>(new_object.value_len) /
                                  static_cast<double>(ec_schema_.k));
    block_size = 64 * std::ceil(static_cast<double>(block_size) / 64.0);

    auto &stripe = new_stripe_item(block_size);
    new_object.stripes.push_back(stripe.stripe_id);

    generate_placement_plan(stripe.nodes, stripe.stripe_id);

    init_placement(placement, key, value_len, block_size, -1);

    placement.stripe_ids.push_back(stripe.stripe_id);
    for (auto &node_id : stripe.nodes) {
      auto &node = node_info_[node_id];
      placement.datanode_ip_port.push_back({node.ip, node.port});
    }
  } else {
    size_t block_size =
        std::ceil(static_cast<double>(ec_schema_.strip_size_upper) /
                  static_cast<double>(ec_schema_.k));
    block_size = 64 * std::ceil(static_cast<double>(block_size) / 64.0);

    init_placement(placement, key, value_len, block_size, -1);

    int num_of_stripes = value_len / (ec_schema_.k * block_size);
    size_t left_value_len = value_len;
    for (int i = 0; i < num_of_stripes; i++) {
      left_value_len -= ec_schema_.k * block_size;

      auto &stripe = new_stripe_item(block_size);
      new_object.stripes.push_back(stripe.stripe_id);

      generate_placement_plan(stripe.nodes, stripe.stripe_id);

      placement.stripe_ids.push_back(stripe.stripe_id);
      for (auto &node_id : stripe.nodes) {
        auto &node = node_info_[node_id];
        placement.datanode_ip_port.push_back({node.ip, node.port});
      }
    }
    if (left_value_len > 0) {
      size_t tail_block_size = std::ceil(static_cast<double>(left_value_len) /
                                         static_cast<double>(ec_schema_.k));
      tail_block_size =
          64 * std::ceil(static_cast<double>(tail_block_size) / 64.0);

      placement.tail_block_size = tail_block_size;
      auto &stripe = new_stripe_item(tail_block_size);
      new_object.stripes.push_back(stripe.stripe_id);

      generate_placement_plan(stripe.nodes, stripe.stripe_id);

      placement.stripe_ids.push_back(stripe.stripe_id);
      for (auto &node_id : stripe.nodes) {
        auto &node = node_info_[node_id];
        placement.datanode_ip_port.push_back({node.ip, node.port});
      }
    } else {
      placement.tail_block_size = -1;
    }
  }

  mutex_.lock();
  objects_waiting_commit_[key] = new_object;
  mutex_.unlock();

  std::pair<std::string, int> proxy_location;
  unsigned int selected_cluster_id = random_index(cluster_info_.size());
  std::string selected_proxy_ip = cluster_info_[selected_cluster_id].proxy_ip;
  int selected_proxy_port = cluster_info_[selected_cluster_id].proxy_port;
  proxy_location = {selected_proxy_ip, selected_proxy_port};

  async_simple::coro::syncAwait(
      proxys_[selected_proxy_ip + std::to_string(selected_proxy_port)]
          ->call<&Proxy::start_encode_and_store_object>(placement));

  return proxy_location;
}

void Coordinator::generate_placement_plan(std::vector<unsigned int> &nodes,
                                          unsigned int stripe_id) {}

stripe_item &Coordinator::new_stripe_item(size_t block_size) {
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
  stripe.block_size = block_size;

  return stripe_info_[temp.stripe_id];
}

void Coordinator::init_placement(placement_info &placement, std::string key,
                                 size_t value_len, size_t block_size,
                                 size_t tail_block_size) {
  placement.encode_type = ec_schema_.encode_type;
  placement.key = key;
  placement.value_len = value_len;
  placement.k = ec_schema_.k;
  placement.g = ec_schema_.g;
  placement.real_l = ec_schema_.real_l;
  placement.b = ec_schema_.b;
  placement.block_size = block_size;
  placement.tail_block_size = tail_block_size;
}

void Coordinator::connect_to_proxy(std::string ip, int port) {
  std::string location = ip + std::to_string(port);
  my_assert(proxys_.contains(location) == false);

  proxys_[location] = std::make_unique<coro_rpc::coro_rpc_client>();
  async_simple::coro::syncAwait(
      proxys_[location]->connect(ip, std::to_string(port)));
}

bool Coordinator::check_commit(std::string key) {
  std::unique_lock<std::mutex> lck(mutex_);
  while (commited_object_info_.contains(key) == false) {
    cv_.wait(lck);
  }
  return true;
}

void Coordinator::commit_object(std::string key) {
  std::unique_lock<std::mutex> lck(mutex_);
  my_assert(commited_object_info_.contains(key) == false &&
            objects_waiting_commit_.contains(key) == true);
  commited_object_info_[key] = objects_waiting_commit_[key];
  cv_.notify_all();
  objects_waiting_commit_.erase(key);
}

size_t Coordinator::ask_for_data(std::string key, std::string client_ip,
                                 int client_port) {
  mutex_.lock();
  if (commited_object_info_.contains(key) == false) {
    mutex_.unlock();
    my_assert(false);
  }
  meta_info_of_object &object = commited_object_info_[key];
  mutex_.unlock();

  placement_info placement;
  if (object.value_len > ec_schema_.strip_size_upper) {
    size_t block_size =
        std::ceil(static_cast<double>(ec_schema_.strip_size_upper) /
                  static_cast<double>(ec_schema_.k));
    block_size = 64 * std::ceil(static_cast<double>(block_size) / 64.0);

    size_t tail_block_size = -1;
    if (object.value_len % (ec_schema_.k * block_size) != 0) {
      size_t tail_stripe_size = object.value_len % (ec_schema_.k * block_size);
      tail_block_size = std::ceil(static_cast<double>(tail_stripe_size) /
                                  static_cast<double>(ec_schema_.k));
      tail_block_size =
          64 * std::ceil(static_cast<double>(tail_block_size) / 64.0);
    }
    init_placement(placement, key, object.value_len, block_size,
                   tail_block_size);
  } else {
    size_t block_size = std::ceil(static_cast<double>(object.value_len) /
                                  static_cast<double>(ec_schema_.k));
    block_size = 64 * std::ceil(static_cast<double>(block_size) / 64.0);
    init_placement(placement, key, object.value_len, block_size, -1);
  }

  for (auto stripe_id : object.stripes) {
    stripe_item &stripe = stripe_info_[stripe_id];
    placement.stripe_ids.push_back(stripe_id);
    for (auto node_id : stripe.nodes) {
      node_item &node = node_info_[node_id];
      placement.datanode_ip_port.push_back({node.ip, node.port});
    }
  }

  placement.client_ip = client_ip;
  placement.client_port = client_port;

  int selected_proxy_id = random_index(cluster_info_.size());
  std::string location =
      cluster_info_[selected_proxy_id].proxy_ip +
      std::to_string(cluster_info_[selected_proxy_id].proxy_port);
  async_simple::coro::syncAwait(
      proxys_[location]->call<&Proxy::decode_and_transfer_data>(placement));

  return object.value_len;
}