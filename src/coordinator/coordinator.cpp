#include "../../include/coordinator.h"
#include "../../include/tinyxml2.h"

std::string Coordinator::echo(std::string s) { return s + "zhaohao"; }

Coordinator::Coordinator(std::string ip, int port, std::string config_file_path)
    : ip_(ip), port_(port), config_file_path_(config_file_path), alpha_(0.5) {
  rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(1, port_);
  rpc_server_->register_handler<&Coordinator::set_erasure_coding_parameters>(
      this);
  rpc_server_->register_handler<&Coordinator::check_commit>(this);
  rpc_server_->register_handler<&Coordinator::ask_for_data>(this);
  rpc_server_->register_handler<&Coordinator::echo>(this);
  rpc_server_->register_handler<&Coordinator::commit_object>(this);
  rpc_server_->register_handler<&Coordinator::ask_for_repair>(this);

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
    // 注意加1
    connect_to_proxy(cur->second.proxy_ip, cur->second.proxy_port + 1);
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

static bool cmp_prediction_cost(std::pair<std::vector<int>, double> &a,
                                std::pair<std::vector<int>, double> &b) {
  return a.second > b.second;
}

static bool cmp_combined_cost(std::pair<unsigned int, double> &a,
                              std::pair<unsigned int, double> &b) {
  return a.second < b.second;
}

void Coordinator::generate_placement_plan(std::vector<unsigned int> &nodes,
                                          unsigned int stripe_id) {
  // 我的理解，flat放置不用单独实现，在实验室限制服务器网速即可
  // 因此这里没有实现flag放置

  stripe_item &stripe = stripe_info_[stripe_id];
  int k = stripe.k;
  int real_l = stripe.real_l;
  int b = stripe.b;
  int g = stripe.g;
  my_assert(k % real_l == 0);

  if (stripe.placement_type == Placement_Type::random) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned int> dis(0, node_info_.size() - 1);

    std::vector<bool> visited_nodes(node_info_.size(), false);
    // (某个cluster中已经包含的group编号, 某个cluster中已经存放的块数量)
    std::vector<std::pair<std::unordered_set<int>, int>> help(
        cluster_info_.size());
    for (auto i = 0; i < cluster_info_.size(); i++) {
      help[i].second = 0;
    }
    unsigned int node_id;
    unsigned int cluster_id;
    int space_upper;
    auto find_a_node_for_a_block = [&, this]() {
      do {
        node_id = dis(gen);
        cluster_id = node_info_[node_id].cluster_id;
        space_upper = g + help[cluster_id].first.size();
      } while (visited_nodes[node_id] == true ||
               help[cluster_id].second == space_upper);

      my_assert(help[cluster_id].second < space_upper);
      my_assert(visited_nodes[node_id] == false);

      nodes.push_back(node_id);
      node_info_[node_id].stripe_ids.insert(stripe_id);
      visited_nodes[node_id] = true;
      help[cluster_id].second++;

      return cluster_id;
    };

    // 数据块
    for (int i = 0; i < real_l; i++) {
      for (int j = 0; j < b; j++) {
        unsigned int cluster_id = find_a_node_for_a_block();
        help[cluster_id].first.insert(i);
      }
    }

    // 全局校验块
    for (int i = 0; i < g; i++) {
      unsigned int cluster_id = find_a_node_for_a_block();
    }

    // 局部校验块
    for (int i = 0; i < real_l; i++) {
      unsigned int cluster_id = find_a_node_for_a_block();
      help[cluster_id].first.insert(i);
    }
  } else if (stripe.placement_type == Placement_Type::strategy_ECWIDE) {
    // partition plan的每个元素：{num of data, num of local, num of global}
    std::vector<std::vector<int>> partition_plan =
        partition_strategy_ECWIDE(k, g, b);

    // ECWIDE没有考虑load balance，随机选择节点存放
    select_by_random(partition_plan, nodes, stripe_id);
  } else if (stripe.placement_type ==
             Placement_Type::strategy_ICPP23_IGNORE_LOAD) {
    std::vector<std::vector<int>> partition_plan =
        partition_strategy_ICPP23(k, g, b);

    // 不考虑load balance，随机选择节点存放
    select_by_random(partition_plan, nodes, stripe_id);
  } else if (stripe.placement_type ==
             Placement_Type::strategy_ICPP23_CONSIDER_LOAD) {
    std::vector<std::vector<int>> partition_plan =
        partition_strategy_ICPP23(k, g, b);

    // 考虑load balance
    select_by_load(partition_plan, nodes, stripe_id);
  } else {
    my_assert(false);
  }
}

void Coordinator::select_by_load(std::vector<std::vector<int>> &partition_plan,
                                 std::vector<unsigned int> &nodes,
                                 unsigned int stripe_id) {
  std::vector<double> num_of_blocks_each_par;
  std::vector<double> num_of_data_blocks_each_par;
  for (auto &partition : partition_plan) {
    num_of_blocks_each_par.push_back(partition[0] + partition[1] +
                                     partition[2]);
    num_of_data_blocks_each_par.push_back(partition[0]);
  }

  double avg_blocks = 0;
  double avg_data_blocks = 0;
  for (auto i = 0; i < partition_plan.size(); i++) {
    avg_blocks += num_of_blocks_each_par[i];
    avg_data_blocks += num_of_data_blocks_each_par[i];
  }
  avg_blocks = avg_blocks / (double)num_of_blocks_each_par.size();
  avg_data_blocks =
      avg_data_blocks / (double)num_of_data_blocks_each_par.size();

  std::vector<std::pair<std::vector<int>, double>> prediction_cost_each_par;
  for (auto i = 0; i < partition_plan.size(); i++) {
    double storage_cost = num_of_blocks_each_par[i] / avg_blocks;
    double network_cost = num_of_data_blocks_each_par[i] / avg_data_blocks;
    double prediction_cost =
        storage_cost * (1 - alpha_) + network_cost * alpha_;
    prediction_cost_each_par.push_back({partition_plan[i], prediction_cost});
  }
  // 将partition按预计开销降序排列
  std::sort(prediction_cost_each_par.begin(), prediction_cost_each_par.end(),
            cmp_prediction_cost);
  partition_plan.clear();
  for (auto &partition : prediction_cost_each_par) {
    partition_plan.push_back(partition.first);
  }

  int data_block_idx = 0;
  int global_block_idx = stripe_info_[stripe_id].k;
  int local_block_idx = stripe_info_[stripe_id].k + stripe_info_[stripe_id].g;

  double node_avg_storage_cost, node_avg_network_cost;
  double cluster_avg_storage_cost, cluster_avg_network_cost;
  compute_avg_cost_for_each_node_and_cluster(
      node_avg_storage_cost, node_avg_network_cost, cluster_avg_storage_cost,
      cluster_avg_network_cost);

  std::vector<std::pair<unsigned int, double>> sorted_clusters;
  for (auto &cluster : cluster_info_) {
    double cluster_storage_cost, cluster_network_cost;
    compute_total_cost_for_cluster(cluster.second, cluster_storage_cost,
                                   cluster_network_cost);
    double combined_cost =
        (cluster_storage_cost / cluster_avg_storage_cost) * (1 - alpha_) +
        (cluster_network_cost / cluster_avg_network_cost) * alpha_;
    sorted_clusters.push_back({cluster.first, combined_cost});
  }
  std::sort(sorted_clusters.begin(), sorted_clusters.end(), cmp_combined_cost);

  int cluster_idx = 0;
  for (auto i = 0; i < partition_plan.size(); i++) {
    cluster_item &cluster = cluster_info_[sorted_clusters[cluster_idx++].first];

    std::vector<std::pair<unsigned int, double>> sorted_nodes_in_each_cluster;
    for (auto &node_id : cluster.nodes) {
      node_item &node = node_info_[node_id];
      double node_storage_cost = node.storage_cost / node.storage;
      double node_network_cost = node.network_cost / node.bandwidth;
      double combined_cost =
          (node_storage_cost / node_avg_storage_cost) * (1 - alpha_) +
          (node_network_cost / node_avg_network_cost) * alpha_;
      sorted_nodes_in_each_cluster.push_back({node_id, combined_cost});
    }
    std::sort(sorted_nodes_in_each_cluster.begin(),
              sorted_nodes_in_each_cluster.end(), cmp_combined_cost);

    int node_idx = 0;
    // data
    for (int j = 0; j < partition_plan[i][0]; j++) {
      int node_id = sorted_nodes_in_each_cluster[node_idx++].first;
      nodes[data_block_idx++] = node_id;
      node_info_[node_id].stripe_ids.insert(stripe_id);
    }
    // local
    for (int j = 0; j < partition_plan[i][1]; j++) {
      int node_id = sorted_nodes_in_each_cluster[node_idx++].first;
      nodes[local_block_idx++] = node_id;
      node_info_[node_id].stripe_ids.insert(stripe_id);
    }
    // global
    for (int j = 0; j < partition_plan[i][2]; j++) {
      int node_id = sorted_nodes_in_each_cluster[node_idx++].first;
      nodes[global_block_idx++] = node_id;
      node_info_[node_id].stripe_ids.insert(stripe_id);
    }
  }
}

void Coordinator::compute_avg_cost_for_each_node_and_cluster(
    double &node_avg_storage_cost, double &node_avg_network_cost,
    double &cluster_avg_storage_cost, double &cluster_avg_network_cost) {
  for (auto &node : node_info_) {
    double storage_cost = node.second.storage_cost / node.second.storage;
    double network_cost = node.second.network_cost / node.second.bandwidth;
    node_avg_storage_cost += storage_cost;
    node_avg_network_cost += network_cost;
  }
  node_avg_storage_cost /= (double)node_info_.size();
  node_avg_network_cost /= (double)node_info_.size();

  for (auto &cluster : cluster_info_) {
    double storage_cost = 0, network_cost = 0;
    compute_total_cost_for_cluster(cluster.second, storage_cost, network_cost);
    cluster_avg_storage_cost += storage_cost;
    cluster_avg_network_cost += network_cost;
  }
  cluster_avg_storage_cost /= (double)cluster_info_.size();
  cluster_avg_network_cost /= (double)cluster_info_.size();
}

void Coordinator::compute_total_cost_for_cluster(cluster_item &cluster,
                                                 double &storage_cost,
                                                 double &network_cost) {
  double all_storage = 0, all_bandwidth = 0;
  double all_storage_cost = 0, all_network_cost = 0;
  for (auto i = 0; i < cluster.nodes.size(); i++) {
    unsigned int node_id = cluster.nodes[i];
    node_item &node = node_info_[node_id];
    all_storage += node.storage;
    all_bandwidth += node.bandwidth;
    all_storage_cost += node.storage_cost;
    all_network_cost += node.network_cost;
  }
  storage_cost = all_storage_cost / all_storage;
  network_cost = all_network_cost / all_bandwidth;
}

void Coordinator::select_by_random(
    std::vector<std::vector<int>> &partition_plan,
    std::vector<unsigned int> &nodes, unsigned int stripe_id) {
  int data_block_idx = 0;
  int global_block_idx = stripe_info_[stripe_id].k;
  int local_block_idx = stripe_info_[stripe_id].k + stripe_info_[stripe_id].g;

  std::random_device rd_cluster;
  std::mt19937 gen_cluster(rd_cluster());
  std::uniform_int_distribution<unsigned int> dis_cluster(
      0, cluster_info_.size() - 1);

  std::vector<bool> visited_clusters(cluster_info_.size(), false);

  for (auto i = 0; i < partition_plan.size(); i++) {
    unsigned int cluster_id;
    do {
      cluster_id = dis_cluster(gen_cluster);
    } while (visited_clusters[cluster_id] == true);
    visited_clusters[cluster_id] = true;
    cluster_item &cluster = cluster_info_[cluster_id];

    std::random_device rd_node;
    std::mt19937 gen_node(rd_node());
    std::uniform_int_distribution<unsigned int> dis_node(
        0, cluster.nodes.size() - 1);

    std::vector<bool> visited_nodes(cluster.nodes.size(), false);

    auto find_a_node_for_a_block = [&, this](int &block_idx) {
      int node_idx;
      do {
        // 注意，此处是node_idx，而非node_id
        // cluster.nodes[node_idx]才是node_id
        node_idx = dis_node(gen_node);
      } while (visited_nodes[node_idx] == true);
      visited_nodes[node_idx] = true;
      nodes[block_idx++] = cluster.nodes[node_idx];
      node_info_[cluster.nodes[node_idx]].stripe_ids.insert(stripe_id);
    };

    for (int j = 0; j < partition_plan[i][0]; j++) {
      find_a_node_for_a_block(data_block_idx);
    }

    for (int j = 0; j < partition_plan[i][1]; j++) {
      find_a_node_for_a_block(local_block_idx);
    }

    for (int j = 0; j < partition_plan[i][2]; j++) {
      find_a_node_for_a_block(global_block_idx);
    }
  }
}

std::vector<std::vector<int>>
Coordinator::placement_strategy_optimal_data_block_repair(int k, int g, int b) {
  std::vector<std::vector<int>> partition_plan;
  int real_l = k / b;

  int left_data_block_in_each_group = b;
  if (b >= g + 1) {
    for (int i = 0; i < real_l; i++) {
      int num_of_left_data_block_in_cur_group = b;
      while (num_of_left_data_block_in_cur_group >= g + 1) {
        partition_plan.push_back({g + 1, 0, 0});
        num_of_left_data_block_in_cur_group -= (g + 1);
      }
      left_data_block_in_each_group = num_of_left_data_block_in_cur_group;
    }
  }

  assert(left_data_block_in_each_group == b % (g + 1));

  if (left_data_block_in_each_group == 0) {
    partition_plan.push_back({0, real_l, 0});
  } else {
    int theta = g / left_data_block_in_each_group;
    int num_of_left_group = real_l;
    while (num_of_left_group >= theta) {
      partition_plan.push_back(
          {theta * left_data_block_in_each_group, theta, 0});
      num_of_left_group -= theta;
    }
    if (num_of_left_group > 0) {
      partition_plan.push_back(
          {num_of_left_group * left_data_block_in_each_group, num_of_left_group,
           0});
    }
  }

  return partition_plan;
}

std::vector<std::vector<int>>
Coordinator::partition_strategy_ECWIDE(int k, int g, int b) {
  auto partition_plan = placement_strategy_optimal_data_block_repair(k, g, b);

  // ECWIDE将全局校验块单独放到了1个cluster
  partition_plan.push_back({0, 0, g});

  return partition_plan;
}

static bool left_space_cmp(std::pair<int, int> &a, std::pair<int, int> &b) {
  return a.second > b.second;
}

std::vector<std::vector<int>>
Coordinator::partition_strategy_ICPP23(int k, int g, int b) {
  auto partition_plan = placement_strategy_optimal_data_block_repair(k, g, b);

  std::vector<std::pair<int, int>> space_left_in_each_partition;
  int sum_left_space = 0;
  // 遍历每个partition查看剩余空间
  for (auto i = 0; i < partition_plan.size(); i++) {
    int num_of_group = partition_plan[i][1];
    // 若某个partition不包含局部校验块，说明这里所有块属于1个group
    if (partition_plan[i][1] == 0) {
      num_of_group = 1;
    }
    int max_space = g + num_of_group;
    int left_space = max_space - partition_plan[i][0] - partition_plan[i][1];
    space_left_in_each_partition.push_back({i, left_space});
    sum_left_space += left_space;
  }

  // 用全局校验块填充剩余空间
  int left_g = g;
  if (sum_left_space >= g) {
    std::sort(space_left_in_each_partition.begin(),
              space_left_in_each_partition.end(), left_space_cmp);
    for (auto i = 0; i < space_left_in_each_partition.size() && left_g > 0;
         i++) {
      if (space_left_in_each_partition[i].second > 0) {
        int j = space_left_in_each_partition[i].first;
        if (left_g >= space_left_in_each_partition[i].second) {
          partition_plan[j][2] = space_left_in_each_partition[i].second;
          left_g -= partition_plan[j][2];
        } else {
          partition_plan[j][2] = left_g;
          left_g -= left_g;
        }
      }
    }
    my_assert(left_g == 0);
  } else {
    partition_plan.push_back({0, 0, left_g});
  }

  return partition_plan;
}

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

void Coordinator::ask_for_repair(std::vector<unsigned int> failed_node_ids) {
  // 目前仅实现了single node repair
  my_assert(failed_node_ids.size() == 1);

  // 找到所有数据损坏的条带
  std::unordered_set<unsigned int> failed_stripe_ids;
  for (auto node_id : failed_node_ids) {
    for (auto stripe_id : node_info_[node_id].stripe_ids) {
      failed_stripe_ids.insert(stripe_id);
    }
  }

  for (auto stripe_id : failed_stripe_ids) {
    // 找到条带内的哪一个block损坏了
    std::vector<int> failed_block_indexes;
    for (auto i = 0; i < stripe_info_[stripe_id].nodes.size(); i++) {
      if (stripe_info_[stripe_id].nodes[i] == failed_node_ids[0]) {
        failed_block_indexes.push_back(i);
      }
    }
    my_assert(failed_block_indexes.size() == 1);

    if (stripe_info_[stripe_id].encode_type == Encode_Type::Azure_LRC) {
      do_repair(stripe_id, {failed_block_indexes[0]});
    }
  }
}

void Coordinator::do_repair(unsigned int stripe_id,
                            std::vector<int> failed_block_indexes) {
  my_assert(failed_block_indexes.size() == 1);

  // 记录了本次修复涉及的cluster id
  std::vector<unsigned int> repair_span_cluster;
  // 记录了需要从哪些cluster中，读取哪些block，记录顺序和repair_span_cluster对应
  // cluster_id->vector((node_ip, node_port), block_index)
  std::vector<std::vector<std::pair<std::pair<std::string, int>, int>>>
      blocks_to_read_in_each_cluster;
  // 记录了修复后block应存放的位置
  // (node_id, block_index)
  std::vector<std::pair<unsigned int, int>> new_locations_with_block_index;
  generate_repair_plan(stripe_id, failed_block_indexes,
                       blocks_to_read_in_each_cluster, repair_span_cluster,
                       new_locations_with_block_index);

  my_assert(repair_span_cluster.size() > 0);
  unsigned int main_cluster_id = repair_span_cluster[0];

  std::vector<std::thread> repairers;
  for (auto i = 0; i < repair_span_cluster.size(); i++) {
    if (i == 0) {
      // main cluster
      my_assert(repair_span_cluster[i] == main_cluster_id);
      repairers.push_back(std::thread([&, this, i, main_cluster_id] {
        stripe_item &stripe = stripe_info_[stripe_id];
        main_repair_plan repair_plan;
        repair_plan.k = stripe.k;
        repair_plan.real_l = stripe.real_l;
        repair_plan.g = stripe.g;
        repair_plan.b = stripe.b;
        repair_plan.encode_type = stripe.encode_type;
        repair_plan.partial_decoding = ec_schema_.partial_decoding;
        repair_plan.multi_clusters_involved = (repair_span_cluster.size() > 1);
        repair_plan.block_size = stripe.block_size;
        repair_plan.stripe_id = stripe.stripe_id;
        repair_plan.cluster_id = main_cluster_id;

        repair_plan.inner_cluster_help_blocks_info =
            blocks_to_read_in_each_cluster[0];
        for (auto j = 0; j < blocks_to_read_in_each_cluster.size(); j++) {
          for (auto t = 0; t < blocks_to_read_in_each_cluster[j].size(); t++) {
            repair_plan.live_blocks_index.push_back(
                blocks_to_read_in_each_cluster[j][t].second);
          }
        }
        repair_plan.failed_blocks_index = failed_block_indexes;

        my_assert(new_locations_with_block_index.size() == 1);
        node_item &node = node_info_[new_locations_with_block_index[0].first];
        repair_plan.new_locations.push_back(
            {{node.ip, node.port}, new_locations_with_block_index[0].second});

        for (auto cluster_id : repair_span_cluster) {
          if (cluster_id != main_cluster_id) {
            repair_plan.help_cluster_ids.push_back(cluster_id);
          }
        }

        std::string proxy_ip = cluster_info_[main_cluster_id].proxy_ip;
        int port = cluster_info_[main_cluster_id].proxy_port;
        // 注意port + 1
        async_simple::coro::syncAwait(
            proxys_[proxy_ip + std::to_string(port + 1)]
                ->call<&Proxy::main_repair>(repair_plan));
      }));
    } else {
      // help cluster
      unsigned cluster_id = repair_span_cluster[i];
      repairers.push_back(std::thread([&, this, i, cluster_id]() {
        stripe_item &stripe = stripe_info_[stripe_id];
        help_repair_plan repair_plan;
        repair_plan.k = stripe.k;
        repair_plan.real_l = stripe.real_l;
        repair_plan.g = stripe.g;
        repair_plan.b = stripe.b;
        repair_plan.encode_type = stripe.encode_type;
        repair_plan.partial_decoding = ec_schema_.partial_decoding;
        repair_plan.multi_clusters_involved = (repair_span_cluster.size() > 1);
        repair_plan.block_size = stripe.block_size;
        repair_plan.stripe_id = stripe.stripe_id;
        repair_plan.cluster_id = cluster_id;

        repair_plan.inner_cluster_help_blocks_info =
            blocks_to_read_in_each_cluster[i];
        for (auto j = 0; j < blocks_to_read_in_each_cluster.size(); j++) {
          for (auto t = 0; t < blocks_to_read_in_each_cluster[j].size(); t++) {
            repair_plan.live_blocks_index.push_back(
                blocks_to_read_in_each_cluster[j][t].second);
          }
        }
        repair_plan.failed_blocks_index = failed_block_indexes;

        repair_plan.proxy_ip = cluster_info_[cluster_id].proxy_ip;
        repair_plan.proxy_port = cluster_info_[cluster_id].proxy_port;
        // 注意port + 1
        async_simple::coro::syncAwait(
            proxys_[repair_plan.proxy_ip +
                    std::to_string(repair_plan.proxy_port + 1)]
                ->call<&Proxy::help_repair>(repair_plan));
      }));
    }
    for (auto i = 0; i < repairers.size(); i++) {
      repairers[i].join();
    }
  }
  // stripe_info_[stripe_id].nodes[new_locations_with_block_index[0].second] =
  //     new_locations_with_block_index[0].first;
}

static bool cmp_num_live_blocks(std::pair<unsigned int, std::vector<int>> &a,
                                std::pair<unsigned int, std::vector<int>> &b) {
  return a.second.size() > b.second.size();
}

void Coordinator::generate_repair_plan(
    unsigned int stripe_id, std::vector<int> &failed_block_indexes,
    std::vector<std::vector<std::pair<std::pair<std::string, int>, int>>>
        &blocks_to_read_in_each_cluster,
    std::vector<unsigned int> &repair_span_cluster,
    std::vector<std::pair<unsigned int, int>> &new_locations_with_block_index) {
  stripe_item &stripe = stripe_info_[stripe_id];
  int k = stripe.k;
  int real_l = stripe.real_l;
  int g = stripe.g;
  int b = stripe.b;

  int failed_block_index = failed_block_indexes[0];
  node_item &failed_node = node_info_[stripe.nodes[failed_block_index]];
  unsigned int main_cluster_id = failed_node.cluster_id;
  repair_span_cluster.push_back(main_cluster_id);

  // 将修复好的块放回原位
  new_locations_with_block_index.push_back(
      {failed_node.node_id, failed_block_index});

  my_assert(failed_block_index >= 0 &&
            failed_block_index <= (k + g + real_l - 1));
  if (failed_block_index >= k && failed_block_index <= (k + g - 1)) {
    // 修复全局校验块
    std::unordered_map<unsigned int, std::vector<int>>
        live_blocks_in_each_cluster;
    // 找到每个cluster中的存活块
    for (int i = 0; i < (k + g - 1); i++) {
      if (i != failed_block_index) {
        node_item &live_node = node_info_[i];
        live_blocks_in_each_cluster[live_node.cluster_id].push_back(i);
      }
    }

    std::unordered_map<unsigned int, std::vector<int>>
        live_blocks_needed_in_each_cluster;
    int num_of_needed_live_blocks = k;
    // 优先读取main cluster的，即损坏块所在cluster
    for (auto live_block_index : live_blocks_in_each_cluster[main_cluster_id]) {
      if (num_of_needed_live_blocks <= 0) {
        break;
      }
      live_blocks_needed_in_each_cluster[main_cluster_id].push_back(
          live_block_index);
      num_of_needed_live_blocks--;
    }

    // 需要对剩下的cluster中存活块的数量进行排序，优先从存活块数量多的cluster中读取
    std::vector<std::pair<unsigned int, std::vector<int>>>
        sorted_live_blocks_in_each_cluster;
    for (auto &cluster : live_blocks_in_each_cluster) {
      if (cluster.first != main_cluster_id) {
        sorted_live_blocks_in_each_cluster.push_back(
            {cluster.first, cluster.second});
      }
    }
    std::sort(sorted_live_blocks_in_each_cluster.begin(),
              sorted_live_blocks_in_each_cluster.end(), cmp_num_live_blocks);
    for (auto &cluster : sorted_live_blocks_in_each_cluster) {
      for (auto &block_index : cluster.second) {
        if (num_of_needed_live_blocks <= 0) {
          break;
        }
        live_blocks_needed_in_each_cluster[cluster.first].push_back(
            block_index);
        num_of_needed_live_blocks--;
      }
    }

    // 记录需要从main cluster中读取的存活块
    std::vector<std::pair<std::pair<std::string, int>, int>>
        blocks_to_read_in_main_cluster;
    for (auto &block_index :
         live_blocks_needed_in_each_cluster[main_cluster_id]) {
      node_item &node = node_info_[stripe.nodes[block_index]];
      blocks_to_read_in_main_cluster.push_back(
          {{node.ip, node.port}, block_index});
    }
    blocks_to_read_in_each_cluster.push_back(blocks_to_read_in_main_cluster);

    // 记录需要从其它cluster中读取的存活块
    for (auto &cluster : live_blocks_needed_in_each_cluster) {
      if (cluster.first != main_cluster_id) {
        repair_span_cluster.push_back(cluster.first);

        std::vector<std::pair<std::pair<std::string, int>, int>>
            blocks_to_read_in_another_cluster;
        for (auto &block_index : cluster.second) {
          node_item &node = node_info_[stripe.nodes[block_index]];
          blocks_to_read_in_another_cluster.push_back(
              {{node.ip, node.port}, block_index});
        }
        blocks_to_read_in_each_cluster.push_back(
            blocks_to_read_in_another_cluster);
      }
    }
  } else {
    // 修复数据块和局部校验块
    int group_index = -1;
    if (failed_block_index >= 0 && failed_block_index <= (k - 1)) {
      group_index = failed_block_index / b;
    } else {
      group_index = failed_block_index - (k + g);
    }

    std::vector<std::pair<unsigned int, int>> live_blocks_in_group;
    for (int i = 0; i < b; i++) {
      int block_index = group_index * b + i;
      if (block_index != failed_block_index) {
        if (block_index >= k) {
          break;
        }
        live_blocks_in_group.push_back(
            {stripe.nodes[block_index], block_index});
      }
    }
    if (failed_block_index != k + g + group_index) {
      live_blocks_in_group.push_back(
          {stripe.nodes[k + g + group_index], k + g + group_index});
    }

    std::unordered_set<unsigned int> span_cluster;
    for (auto &live_block : live_blocks_in_group) {
      span_cluster.insert(node_info_[live_block.first].cluster_id);
    }
    for (auto &cluster_involved : span_cluster) {
      if (cluster_involved != main_cluster_id) {
        repair_span_cluster.push_back(cluster_involved);
      }
    }

    for (auto &cluster_id : repair_span_cluster) {
      std::vector<std::pair<std::pair<std::string, int>, int>>
          blocks_to_read_in_cur_cluster;
      for (auto &live_block : live_blocks_in_group) {
        node_item &node = node_info_[live_block.first];
        if (node.cluster_id == cluster_id) {
          blocks_to_read_in_cur_cluster.push_back(
              {{node.ip, node.port}, live_block.second});
        }
      }
      if (cluster_id == main_cluster_id) {
        my_assert(blocks_to_read_in_cur_cluster.size() > 0);
      }
      blocks_to_read_in_each_cluster.push_back(blocks_to_read_in_cur_cluster);
    }
  }
}