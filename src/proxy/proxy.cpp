#include "../../include/proxy.h"
#include "../../include/coordinator.h"
#include "../../include/erasure_code.h"
#include <string>
#include <thread>
#include <unordered_map>

Proxy::Proxy(std::string ip, int port, std::string coordinator_ip,
             int coordinator_port)
    : ip_(ip), port_for_rpc_(port + 1), port_for_transfer_data_(port),
      coordinator_ip_(coordinator_ip), coordinator_port_(coordinator_port),
      acceptor_(io_context_, asio::ip::tcp::endpoint(
                                 asio::ip::address::from_string(ip.c_str()),
                                 port_for_transfer_data_)) {
  rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(1, port_for_rpc_);
  rpc_server_->register_handler<&Proxy::start_encode_and_store_object>(this);
  rpc_server_->register_handler<&Proxy::decode_and_transfer_data>(this);
  rpc_server_->register_handler<&Proxy::main_repair>(this);
  rpc_server_->register_handler<&Proxy::help_repair>(this);

  rpc_coordinator_ = std::make_unique<coro_rpc::coro_rpc_client>();
  async_simple::coro::syncAwait(rpc_coordinator_->connect(
      coordinator_ip_, std::to_string(coordinator_port_)));
}

Proxy::~Proxy() {
  acceptor_.close();
  rpc_server_->stop();
}

void Proxy::start() { auto err = rpc_server_->start(); }

// 非阻塞的，会立即返回
void Proxy::start_encode_and_store_object(placement_info placement) {
  auto encode_and_store = [this, placement]() {
    asio::ip::tcp::socket peer(io_context_);
    acceptor_.accept(peer);

    size_t value_buf_size =
        placement.k * placement.block_size * placement.stripe_ids.size();
    std::string key_buf(placement.key.size(), 0);
    std::string value_buf(value_buf_size, 0);
    my_assert(key_buf.size() == placement.key.size());
    my_assert(value_buf.size() == value_buf_size);

    size_t readed_len_of_key =
        asio::read(peer, asio::buffer(key_buf.data(), key_buf.size()));
    my_assert(readed_len_of_key == key_buf.size());

    size_t readed_len_of_value =
        asio::read(peer, asio::buffer(value_buf.data(), value_buf.size()));
    my_assert(readed_len_of_value == value_buf.size());

    asio::error_code ignore_ec;
    peer.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
    peer.close(ignore_ec);

    char *object_value = value_buf.data();
    for (auto i = 0; i < placement.stripe_ids.size(); i++) {
      std::vector<char *> data_v(placement.k);
      std::vector<char *> coding_v(placement.g + placement.real_l);
      char **data = (char **)data_v.data();
      char **coding = (char **)coding_v.data();

      size_t cur_block_size;
      if ((i == placement.stripe_ids.size() - 1) &&
          placement.tail_block_size != -1) {
        cur_block_size = placement.tail_block_size;
      } else {
        cur_block_size = placement.block_size;
      }
      my_assert(cur_block_size > 0);

      std::vector<std::vector<char>> space_for_parity_blocks(
          placement.g + placement.real_l, std::vector<char>(cur_block_size));
      for (int j = 0; j < placement.k; j++) {
        data[j] = &object_value[j * cur_block_size];
      }
      for (int j = 0; j < placement.g + placement.real_l; j++) {
        coding[j] = space_for_parity_blocks[j].data();
      }

      encode(placement.k, placement.g, placement.real_l, data, coding,
             cur_block_size, placement.encode_type);

      int num_of_datanodes_involved =
          placement.k + placement.g + placement.real_l;
      int num_of_blocks_each_stripe = num_of_datanodes_involved;
      std::vector<std::thread> writers;
      int k = placement.k;
      for (int j = 0; j < num_of_datanodes_involved; j++) {
        // 当某个block被实际写入memcahced或redis这样的kv存储系统时,
        // key为block_id
        std::string block_id =
            std::to_string(placement.stripe_ids[i] * 1000 + j);
        std::pair<std::string, int> ip_and_port_of_datanode =
            placement.datanode_ip_port[i * num_of_blocks_each_stripe + j];
        writers.push_back(
            std::thread([this, j, k, block_id, data, coding, cur_block_size,
                         ip_and_port_of_datanode]() {
              if (j < k) {
                write_to_datanode(block_id.c_str(), block_id.size(), data[j],
                                  cur_block_size,
                                  ip_and_port_of_datanode.first.c_str(),
                                  ip_and_port_of_datanode.second);
              } else {
                write_to_datanode(block_id.c_str(), block_id.size(),
                                  coding[j - k], cur_block_size,
                                  ip_and_port_of_datanode.first.c_str(),
                                  ip_and_port_of_datanode.second);
              }
            }));
      }
      for (auto j = 0; j < writers.size(); j++) {
        writers[j].join();
      }

      object_value += (placement.k * cur_block_size);
    }

    async_simple::coro::syncAwait(
        rpc_coordinator_->call<&Coordinator::commit_object>(placement.key));
  };

  std::thread new_thread(encode_and_store);
  new_thread.detach();
}

// 非阻塞的，会立即返回
void Proxy::decode_and_transfer_data(placement_info placement) {
  auto decode_and_transfer = [this, placement]() {
    std::string object_value;
    for (auto i = 0; i < placement.stripe_ids.size(); i++) {
      unsigned int stripe_id = placement.stripe_ids[i];
      auto blocks_ptr =
          std::make_shared<std::unordered_map<int, std::string>>();

      size_t cur_block_size;
      if ((i == placement.stripe_ids.size() - 1) &&
          placement.tail_block_size != -1) {
        cur_block_size = placement.tail_block_size;
      } else {
        cur_block_size = placement.block_size;
      }
      my_assert(cur_block_size > 0);

      // 读取前k个数据块即可恢复出原始数据
      int num_of_datanodes_involved = placement.k;
      int num_of_blocks_each_stripe =
          placement.k + placement.g + placement.real_l;
      std::vector<std::thread> readers;
      for (int j = 0; j < num_of_datanodes_involved; j++) {
        std::pair<std::string, int> ip_and_port_of_datanode =
            placement.datanode_ip_port[i * num_of_blocks_each_stripe + j];
        readers.push_back(
            std::thread([this, j, stripe_id, blocks_ptr, cur_block_size,
                         ip_and_port_of_datanode]() {
              std::string block_id = std::to_string(stripe_id * 1000 + j);
              std::string block(cur_block_size, 0);
              read_from_datanode(block_id.c_str(), block_id.size(),
                                 block.data(), cur_block_size,
                                 ip_and_port_of_datanode.first.c_str(),
                                 ip_and_port_of_datanode.second);

              mutex_.lock();

              (*blocks_ptr)[j] = block;

              mutex_.unlock();
            }));
      }
      for (auto j = 0; j < readers.size(); j++) {
        readers[j].join();
      }

      my_assert(blocks_ptr->size() == num_of_datanodes_involved);

      for (int j = 0; j < placement.k; j++) {
        object_value += (*blocks_ptr)[j];
      }
    }

    asio::ip::tcp::socket peer(io_context_);
    asio::ip::tcp::endpoint endpoint(
        asio::ip::make_address(placement.client_ip), placement.client_port);
    peer.connect(endpoint);

    asio::write(peer, asio::buffer(placement.key, placement.key.size()));
    asio::write(peer, asio::buffer(object_value, object_value.size()));

    asio::error_code ignore_ec;
    peer.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
    peer.close(ignore_ec);
  };

  std::thread new_thread(decode_and_transfer);
  new_thread.detach();
}

void Proxy::write_to_datanode(const char *key, size_t key_len,
                              const char *value, size_t value_len,
                              const char *ip, int port) {
  asio::ip::tcp::socket peer(io_context_);
  asio::ip::tcp::endpoint endpoint(asio::ip::make_address(ip), port);
  peer.connect(endpoint);

  int flag = 0;
  std::vector<unsigned char> flag_buf = int_to_bytes(flag);
  asio::write(peer, asio::buffer(flag_buf, flag_buf.size()));

  std::vector<unsigned char> key_size_buf = int_to_bytes(key_len);
  asio::write(peer, asio::buffer(key_size_buf, key_size_buf.size()));

  std::vector<unsigned char> value_size_buf = int_to_bytes(value_len);
  asio::write(peer, asio::buffer(value_size_buf, value_size_buf.size()));

  asio::write(peer, asio::buffer(key, key_len));
  asio::write(peer, asio::buffer(value, value_len));

  std::vector<char> finish(1);
  asio::read(peer, asio::buffer(finish, finish.size()));

  asio::error_code ignore_ec;
  peer.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
  peer.close(ignore_ec);
}

void Proxy::read_from_datanode(const char *key, size_t key_len, char *value,
                               size_t value_len, const char *ip, int port) {
  asio::ip::tcp::socket peer(io_context_);
  asio::ip::tcp::endpoint endpoint(asio::ip::make_address(ip), port);
  peer.connect(endpoint);

  int flag = 1;
  std::vector<unsigned char> flag_buf = int_to_bytes(flag);
  asio::write(peer, asio::buffer(flag_buf, flag_buf.size()));

  std::vector<unsigned char> key_size_buf = int_to_bytes(key_len);
  asio::write(peer, asio::buffer(key_size_buf, key_size_buf.size()));

  asio::write(peer, asio::buffer(key, key_len));

  std::vector<unsigned char> value_buf(value_len);
  asio::read(peer, asio::buffer(value_buf, value_buf.size()));

  asio::error_code ignore_ec;
  peer.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
  peer.close(ignore_ec);
}

void Proxy::main_repair(main_repair_plan repair_plan) {
  std::sort(repair_plan.live_blocks_index.begin(),
            repair_plan.live_blocks_index.end());

  std::vector<std::thread> readers_inner_cluster;
  std::vector<std::thread> readers_outter_cluster;

  int failed_block_index = repair_plan.failed_blocks_index[0];

  // key: cluster_id
  // value：<block_index, block_data>
  std::unordered_map<unsigned int, std::unordered_map<int, std::vector<char>>>
      blocks;

  for (auto i = 0; i < repair_plan.help_cluster_ids.size(); i++) {
    readers_outter_cluster.push_back(std::thread([&, this]() {
      mutex_.lock();
      asio::ip::tcp::socket peer(io_context_);
      acceptor_.accept(peer);
      mutex_.unlock();

      // 读取help cluster id，
      std::vector<unsigned char> cluster_id_buf(sizeof(int));
      asio::read(peer, asio::buffer(cluster_id_buf, cluster_id_buf.size()));
      int help_cluster_id = bytes_to_int(cluster_id_buf);

      if (failed_block_index >= repair_plan.k &&
          failed_block_index <= (repair_plan.k + repair_plan.g - 1)) {
        // 损坏的是全局校验块
        // 此时partial decoding修复操作需要一些较复杂的矩阵运算

        // 读即将传输的block数量
        std::vector<unsigned char> num_of_blocks_buf(sizeof(int));
        asio::read(peer,
                   asio::buffer(num_of_blocks_buf, num_of_blocks_buf.size()));
        int num_of_blocks = bytes_to_int(num_of_blocks_buf);

        // 实际上这里的num_of_blocks只可能是1，因为目前只考虑和实现单块修复流程
        // 读每个block的block index及数据
        for (int j = 0; j < num_of_blocks; j++) {
          std::vector<unsigned char> block_index_buf(sizeof(int));
          asio::read(peer,
                     asio::buffer(block_index_buf, block_index_buf.size()));
          int block_index = bytes_to_int(block_index_buf);

          std::vector<char> block_buf(repair_plan.block_size);
          asio::read(peer, asio::buffer(block_buf, block_buf.size()));

          mutex_.lock();
          blocks[help_cluster_id][block_index] = block_buf;
          mutex_.unlock();
        }
      } else {
        // 损坏的是数据块或局部校验块
        // 存活块直接异或合并即可

        std::vector<char> block_buf(repair_plan.block_size);
        asio::read(peer, asio::buffer(block_buf, block_buf.size()));
        mutex_.lock();
        blocks[help_cluster_id][-1] = block_buf;
        mutex_.unlock();
      }

      asio::error_code ignore_ec;
      peer.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      peer.close(ignore_ec);
    }));
  }
  for (auto i = 0; i < readers_outter_cluster.size(); i++) {
    readers_outter_cluster[i].join();
  }

  for (auto i = 0; i < repair_plan.inner_cluster_help_blocks_info.size(); i++) {
    readers_inner_cluster.push_back(std::thread([&, this, i]() {
      std::string &ip =
          repair_plan.inner_cluster_help_blocks_info[i].first.first;
      int port = repair_plan.inner_cluster_help_blocks_info[i].first.second;
      int block_index = repair_plan.inner_cluster_help_blocks_info[i].second;
      std::vector<char> block_buf(repair_plan.block_size);
      std::string block_id =
          std::to_string(repair_plan.stripe_id * 1000 + block_index);
      size_t temp_size;
      read_from_datanode(block_id.c_str(), block_id.size(), block_buf.data(),
                         repair_plan.block_size, ip.c_str(), port);
      mutex_.lock();
      blocks[repair_plan.cluster_id][block_index] = block_buf;
      mutex_.unlock();
    }));
  }
  for (auto i = 0; i < readers_inner_cluster.size(); i++) {
    readers_inner_cluster[i].join();
  }

  if (failed_block_index >= repair_plan.k &&
      failed_block_index <= (repair_plan.k + repair_plan.g - 1)) {
   // 修复全局校验块
   
  } else {
// 修复数据块或局部校验块
  }
}

void Proxy::help_repair(help_repair_plan repair_plan) {}