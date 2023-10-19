#pragma once

#include "proxy.h"
#include "utils.h"
#include <memory>
#include <mutex>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

class Coordinator {
public:
  Coordinator(std::string ip, int port, std::string config_file_path);
  ~Coordinator();

  void start();

  // rpc调用
  bool set_erasure_coding_parameters(EC_schema ec_schema);
  std::pair<std::string, int> get_proxy_location(std::string key,
                                                 size_t value_len);
  bool check_commit(std::string key);
  void commit_object(std::string key);
  size_t ask_for_data(std::string key, std::string client_ip, int client_port);
  void ask_for_repair(std::vector<unsigned int> failed_node_ids);

  std::string echo(std::string s);

private:
  void generate_placement_plan(std::vector<unsigned int> &nodes,
                               unsigned int stripe_id);
  stripe_item &new_stripe_item(size_t block_size);
  void init_placement(placement_info &placement, std::string key,
                      size_t value_len, size_t block_size,
                      size_t tail_block_size);
  void connect_to_proxy(std::string ip, int port);
  void init_cluster_info();
  void init_proxy_info();
  void do_repair(unsigned int stripe_id, std::vector<int> failed_block_indexes);
  void generate_repair_plan(
      unsigned int stripe_id, std::vector<int> &failed_block_indexes,
      std::vector<std::vector<std::pair<std::pair<std::string, int>, int>>>
          &blocks_to_read_in_each_cluster,
      std::vector<unsigned int> &repair_span_cluster,
      std::vector<std::pair<unsigned int, int>>
          &new_locations_with_block_index);
  std::vector<std::vector<int>> partition_strategy_ECWIDE(int k, int g, int b);
  std::vector<std::vector<int>> partition_strategy_ICPP23(int k, int g, int b);
  std::vector<std::vector<int>>
  placement_strategy_optimal_data_block_repair(int k, int g, int b);
  void select_by_random(std::vector<std::vector<int>> &partition_plan,
                        std::vector<unsigned int> &nodes,
                        unsigned int stripe_id);
  void select_by_load(std::vector<std::vector<int>> &partition_plan,
                      std::vector<unsigned int> &nodes, unsigned int stripe_id);
  void compute_avg_cost_for_each_node_and_cluster(
      double &node_avg_storage_cost, double &node_avg_network_cost,
      double &cluster_avg_storage_cost, double &cluster_avg_network_cost);
  void compute_total_cost_for_cluster(cluster_item &cluster,
                                      double &storage_cost,
                                      double &network_cost);

  std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_{nullptr};
  EC_schema ec_schema_;
  std::unordered_map<unsigned int, stripe_item> stripe_info_;
  std::unordered_map<unsigned int, node_item> node_info_;
  // proxy用于传输数据的port是port
  // 所以cluster_item存的是proxy port
  std::unordered_map<unsigned int, cluster_item> cluster_info_;
  std::unordered_map<std::string, meta_info_of_object> commited_object_info_;
  std::unordered_map<std::string, meta_info_of_object> objects_waiting_commit_;
  std::mutex mutex_;
  std::condition_variable cv_;
  unsigned int next_stripe_id_{0};
  // proxy用于rpc的port是port + 1
  // 这里的key是port + 1，因为在connect时需要用port + 1进行connect
  std::unordered_map<std::string, std::unique_ptr<coro_rpc::coro_rpc_client>>
      proxys_;
  std::string ip_;
  int port_;
  std::string config_file_path_;
  double alpha_;
};