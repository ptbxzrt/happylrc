#include "../../include/proxy.h"
#include "../../include/coordinator.h"
#include "../../include/erasure_code.h"
#include <string>
#include <thread>
#include <unordered_map>

Proxy::Proxy(std::string ip, int port)
    : ip_(ip), port_for_rpc_(port + 1000), port_for_transfer_data_(port),
      acceptor_(io_context_, asio::ip::tcp::endpoint(
                                 asio::ip::address::from_string(ip.c_str()),
                                 port_for_transfer_data_)) {
  rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(1, port_for_rpc_);
  rpc_server_->register_handler<&Proxy::start_encode_and_store_object>(this);
  rpc_server_->register_handler<&Proxy::decode_and_transfer_data>(this);
  rpc_server_->register_handler<&Proxy::main_repair>(this);
  rpc_server_->register_handler<&Proxy::help_repair>(this);
}

Proxy::~Proxy() {
  acceptor_.close();
  rpc_server_->stop();
}

void Proxy::start() { auto err = rpc_server_->start(); }

// 非阻塞的,会立即返回
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
        asio::read(peer, asio::buffer(value_buf.data(), placement.value_len));
    my_assert(readed_len_of_value == placement.value_len);

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

    std::vector<char> finish(1);
    asio::write(peer, asio::buffer(finish, finish.size()));

    asio::error_code ignore_ec;
    peer.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
    peer.close(ignore_ec);
  };

  std::thread new_thread(encode_and_store);
  new_thread.detach();
}

// 非阻塞的,会立即返回
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

  asio::read(peer, asio::buffer(value, value_len));

  asio::error_code ignore_ec;
  peer.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
  peer.close(ignore_ec);
}

void Proxy::main_repair(main_repair_plan repair_plan) {
  my_assert(repair_plan.failed_blocks_index.size() == 1);

  std::sort(repair_plan.live_blocks_index.begin(),
            repair_plan.live_blocks_index.end());
  std::sort(repair_plan.failed_blocks_index.begin(),
            repair_plan.failed_blocks_index.end());

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

      // 读取help cluster id,
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

        // 实际上这里的num_of_blocks只可能是1,因为目前只考虑和实现单块修复流程
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

  std::vector<char> repaired_block(repair_plan.block_size);
  int k = repair_plan.k;
  int real_l = repair_plan.real_l;
  int b = repair_plan.b;
  int g = repair_plan.g;
  if (failed_block_index >= k && failed_block_index <= (k + g - 1)) {
    // 修复全局校验块
    // 因为只考虑单块修复,所以repair_plan.failed_blocks_index的值只可能为1
    // 实际上,这个if语句内的代码逻辑,也适用于多块修复

    // 编码矩阵“去掉”单位矩阵的部分
    std::vector<int> matrix;
    matrix.resize((g + real_l) * k);
    make_lrc_coding_matrix(k, g, real_l, matrix.data());

    // 完整的编码矩阵
    std::vector<int> full_matrix((k + g + real_l) * k, 0);
    for (int i = 0; i < (k + g + real_l); i++) {
      if (i < k) {
        full_matrix[i * k + i] = 1;
      } else {
        for (int j = 0; j < k; j++) {
          full_matrix[i * k + j] = matrix[(i - k) * k + j];
        }
      }
    }

    // 由“编码矩阵中损坏块对应的行”组成的矩阵
    std::vector<int> matrix_failed_block;
    matrix_failed_block.resize(repair_plan.failed_blocks_index.size() * k);
    for (auto i = 0; i < repair_plan.failed_blocks_index.size(); i++) {
      int row = repair_plan.failed_blocks_index[i];
      int *coff = &(full_matrix[row * k]);
      for (int j = 0; j < k; j++) {
        matrix_failed_block[i * k + j] = coff[j];
      }
    }

    // 由“编码矩阵中存活块对应的行”组成的矩阵
    std::vector<int> matrix_live_block;
    matrix_live_block.resize(k * k);
    for (auto i = 0; i < repair_plan.live_blocks_index.size(); i++) {
      int row = repair_plan.live_blocks_index[i];
      int *coff = &(full_matrix[row * k]);
      for (int j = 0; j < k; j++) {
        matrix_live_block[i * k + j] = coff[j];
      }
    }

    // matrix_live_block的逆矩阵
    std::vector<int> invert_matrix_live_block;
    invert_matrix_live_block.resize(k * k);
    jerasure_invert_matrix(matrix_live_block.data(),
                           invert_matrix_live_block.data(), k, 8);

    // help_matrix与存活块再做一定的计算即可修复出损坏块
    int *help_matrix_ptr = jerasure_matrix_multiply(
        matrix_failed_block.data(), invert_matrix_live_block.data(),
        repair_plan.failed_blocks_index.size(), k, k, k, 8);
    std::vector<int> help_matrix;
    help_matrix.resize(repair_plan.failed_blocks_index.size() * k);
    memcpy(help_matrix.data(), help_matrix_ptr,
           help_matrix.size() * sizeof(int));
    free(help_matrix_ptr);

    for (auto &blocks_in_each_cluster : blocks) {
      if (blocks_in_each_cluster.first == repair_plan.cluster_id) {
        // 对从本cluster中读取的块做一个编码合并操作
        // 这个合并操作需要用到刚才计算出来的help_matrix

        std::vector<char> encode_result(repair_plan.block_size);
        std::vector<std::pair<int, std::vector<char>>> saved_encode_result;
        int num_of_live_blocks_in_cur_cluster =
            blocks_in_each_cluster.second.size();
        std::vector<char *> data_v(num_of_live_blocks_in_cur_cluster);
        std::vector<char *> coding_v(1);
        char **data = (char **)data_v.data();
        char **coding = (char **)coding_v.data();

        for (auto i = 0; i < repair_plan.failed_blocks_index.size(); i++) {
          // 这一步的意义是,从help_matrix中找到存活块对应的系数
          int *coff = &(help_matrix[i * k]);
          std::vector<int> coding_matrix(1 * num_of_live_blocks_in_cur_cluster,
                                         1);
          int idx = 0;
          for (auto &block : blocks_in_each_cluster.second) {
            int coff_idx = 0;
            for (; coff_idx < repair_plan.live_blocks_index.size();
                 coff_idx++) {
              if (repair_plan.live_blocks_index[coff_idx] == block.first) {
                break;
              }
            }
            coding_matrix[idx] = coff[coff_idx];
            data[idx] = block.second.data();
            idx++;
          }

          int sum = 0;
          for (auto &num : coding_matrix) {
            sum += num;
          }

          coding[0] = encode_result.data();
          jerasure_matrix_encode(num_of_live_blocks_in_cur_cluster, 1, 8,
                                 coding_matrix.data(), data, coding,
                                 repair_plan.block_size);

          // 这里之所以这样做一个判断,是因为当编码矩阵的元素为0时,Jerasure会立即返回,不做任何计算
          // 但我们则希望编码结果能正常输出1个全0矩阵
          if (sum == 0) {
            encode_result = std::vector<char>(repair_plan.block_size, 0);
          }

          saved_encode_result.push_back(
              {repair_plan.failed_blocks_index[i], encode_result});
        }
        blocks_in_each_cluster.second.clear();
        for (auto &encode_result : saved_encode_result) {
          blocks_in_each_cluster.second[encode_result.first] =
              encode_result.second;
        }
      }
    }

    for (auto i = 0; i < repair_plan.failed_blocks_index.size(); i++) {
      int num_of_clusters_involved = blocks.size();
      std::vector<char *> data_v(num_of_clusters_involved);
      std::vector<char *> coding_v(1);
      char **data = (char **)data_v.data();
      char **coding = (char **)coding_v.data();
      int idx = 0;
      for (auto &blocks_in_each_cluster : blocks) {
        data[idx++] =
            blocks_in_each_cluster.second[repair_plan.failed_blocks_index[i]]
                .data();
      }
      coding[0] = repaired_block.data();
      std::vector<int> new_matrix(1 * num_of_clusters_involved, 1);
      jerasure_matrix_encode(num_of_clusters_involved, 1, 8, new_matrix.data(),
                             data, coding, repair_plan.block_size);
    }
  } else {
    // 修复数据块或局部校验块
    // 直接异或合并即可

    int num_of_blocks_involved = 0;
    for (auto &blocks_in_each_cluster : blocks) {
      num_of_blocks_involved += blocks_in_each_cluster.second.size();
    }

    std::vector<char *> data_v(num_of_blocks_involved);
    std::vector<char *> coding_v(1);
    char **data = (char **)data_v.data();
    char **coding = (char **)coding_v.data();

    int idx = 0;
    for (auto &num_of_blocks_involved : blocks) {
      for (auto &block : num_of_blocks_involved.second) {
        data[idx++] = block.second.data();
      }
    }
    coding[0] = repaired_block.data();
    std::vector<int> new_matrix(1 * num_of_blocks_involved, 1);

    jerasure_matrix_encode(num_of_blocks_involved, 1, 8, new_matrix.data(),
                           data, coding, repair_plan.block_size);
  }

  my_assert(repair_plan.new_locations.size() == 1);
  std::string ip = repair_plan.new_locations[0].first.first;
  int port = repair_plan.new_locations[0].first.second;
  std::string key = std::to_string(repair_plan.stripe_id * 1000 +
                                   repair_plan.failed_blocks_index[0]);
  write_to_datanode(key.data(), key.size(), repaired_block.data(),
                    repaired_block.size(), ip.c_str(), port);
}

void Proxy::help_repair(help_repair_plan repair_plan) {
  std::sort(repair_plan.live_blocks_index.begin(),
            repair_plan.live_blocks_index.end());
  std::sort(repair_plan.failed_blocks_index.begin(),
            repair_plan.failed_blocks_index.end());

  // key: cluster_id
  // value：<block_index, block_data>
  std::unordered_map<unsigned int, std::unordered_map<int, std::vector<char>>>
      blocks;

  std::vector<std::thread> readers_inner_cluster;
  for (auto i = 0; i < repair_plan.inner_cluster_help_blocks_info.size(); i++) {
    readers_inner_cluster.push_back(std::thread([&, this, i]() {
      std::string &ip =
          repair_plan.inner_cluster_help_blocks_info[i].first.first;
      int port = repair_plan.inner_cluster_help_blocks_info[i].first.second;
      int block_idx = repair_plan.inner_cluster_help_blocks_info[i].second;
      std::vector<char> buf(repair_plan.block_size);
      std::string block_id =
          std::to_string(repair_plan.stripe_id * 1000 + block_idx);
      size_t temp_size;
      read_from_datanode(block_id.c_str(), block_id.size(), buf.data(),
                         repair_plan.block_size, ip.c_str(), port);
      mutex_.lock();
      blocks[repair_plan.cluster_id][block_idx] = buf;
      mutex_.unlock();
    }));
  }
  for (auto &thread : readers_inner_cluster) {
    thread.join();
  }

  asio::ip::tcp::socket peer(io_context_);
  asio::ip::tcp::endpoint endpoint(asio::ip::make_address(repair_plan.proxy_ip),
                                   repair_plan.proxy_port);
  peer.connect(endpoint);

  std::vector<unsigned char> cluster_id_buf =
      int_to_bytes(repair_plan.cluster_id);
  asio::write(peer, asio::buffer(cluster_id_buf, cluster_id_buf.size()));

  int k = repair_plan.k;
  int real_l = repair_plan.real_l;
  int b = repair_plan.b;
  int g = repair_plan.g;

  my_assert(repair_plan.failed_blocks_index.size() == 1);
  int failed_blocks_index = repair_plan.failed_blocks_index[0];
  if (failed_blocks_index >= k && failed_blocks_index <= (k + g - 1)) {
    // 损坏块是全局校验块

    std::vector<unsigned char> num_of_blocks_buf =
        int_to_bytes(repair_plan.failed_blocks_index.size());
    asio::write(peer,
                asio::buffer(num_of_blocks_buf, num_of_blocks_buf.size()));

    // 编码矩阵“去掉”单位矩阵的部分
    std::vector<int> matrix;
    matrix.resize((g + real_l) * k);
    make_lrc_coding_matrix(k, g, real_l, matrix.data());

    // 完整的编码矩阵
    std::vector<int> full_matrix((k + g + real_l) * k, 0);
    for (int i = 0; i < (k + g + real_l); i++) {
      if (i < k) {
        full_matrix[i * k + i] = 1;
      } else {
        for (int j = 0; j < k; j++) {
          full_matrix[i * k + j] = matrix[(i - k) * k + j];
        }
      }
    }

    // 由“编码矩阵中损坏块对应的行”组成的矩阵
    std::vector<int> matrix_failed_block;
    matrix_failed_block.resize(repair_plan.failed_blocks_index.size() * k);
    for (auto i = 0; i < repair_plan.failed_blocks_index.size(); i++) {
      int row = repair_plan.failed_blocks_index[i];
      int *coff = &(full_matrix[row * k]);
      for (int j = 0; j < k; j++) {
        matrix_failed_block[i * k + j] = coff[j];
      }
    }

    // 由“编码矩阵中存活块对应的行”组成的矩阵
    std::vector<int> matrix_live_block;
    matrix_live_block.resize(k * k);
    for (auto i = 0; i < repair_plan.live_blocks_index.size(); i++) {
      int row = repair_plan.live_blocks_index[i];
      int *coff = &(full_matrix[row * k]);
      for (int j = 0; j < k; j++) {
        matrix_live_block[i * k + j] = coff[j];
      }
    }

    // matrix_live_block的逆矩阵
    std::vector<int> invert_matrix_live_block;
    invert_matrix_live_block.resize(k * k);
    jerasure_invert_matrix(matrix_live_block.data(),
                           invert_matrix_live_block.data(), k, 8);

    // help_matrix与存活块再做一定的计算即可修复出损坏块
    int *help_matrix_ptr = jerasure_matrix_multiply(
        matrix_failed_block.data(), invert_matrix_live_block.data(),
        repair_plan.failed_blocks_index.size(), k, k, k, 8);
    std::vector<int> help_matrix;
    help_matrix.resize(repair_plan.failed_blocks_index.size() * k);
    memcpy(help_matrix.data(), help_matrix_ptr,
           help_matrix.size() * sizeof(int));
    free(help_matrix_ptr);

    for (auto &blocks_in_each_cluster : blocks) {
      if (blocks_in_each_cluster.first == repair_plan.cluster_id) {
        // 对从本cluster中读取的块做一个编码合并操作
        // 这个合并操作需要用到刚才计算出来的help_matrix

        std::vector<char> encode_result(repair_plan.block_size);
        std::vector<std::pair<int, std::vector<char>>> saved_encode_result;
        int num_of_live_blocks_in_cur_cluster =
            blocks_in_each_cluster.second.size();
        std::vector<char *> data_v(num_of_live_blocks_in_cur_cluster);
        std::vector<char *> coding_v(1);
        char **data = (char **)data_v.data();
        char **coding = (char **)coding_v.data();

        for (auto i = 0; i < repair_plan.failed_blocks_index.size(); i++) {
          // 这一步的意义是,从help_matrix中找到存活块对应的系数
          int *coff = &(help_matrix[i * k]);
          std::vector<int> coding_matrix(1 * num_of_live_blocks_in_cur_cluster,
                                         1);
          int idx = 0;
          for (auto &block : blocks_in_each_cluster.second) {
            int coff_idx = 0;
            for (; coff_idx < repair_plan.live_blocks_index.size();
                 coff_idx++) {
              if (repair_plan.live_blocks_index[coff_idx] == block.first) {
                break;
              }
            }
            coding_matrix[idx] = coff[coff_idx];
            data[idx] = block.second.data();
            idx++;
          }

          int sum = 0;
          for (auto &num : coding_matrix) {
            sum += num;
          }

          coding[0] = encode_result.data();
          jerasure_matrix_encode(num_of_live_blocks_in_cur_cluster, 1, 8,
                                 coding_matrix.data(), data, coding,
                                 repair_plan.block_size);

          // 这里之所以这样做一个判断,是因为当编码矩阵的元素为0时,Jerasure会立即返回,不做任何计算
          // 但我们则希望编码结果能正常输出1个全0矩阵
          if (sum == 0) {
            encode_result = std::vector<char>(repair_plan.block_size, 0);
          }

          std::vector<unsigned char> block_index_buf =
              int_to_bytes(repair_plan.failed_blocks_index[i]);
          asio::write(peer,
                      asio::buffer(block_index_buf, block_index_buf.size()));
          asio::write(peer, asio::buffer(encode_result, encode_result.size()));
        }
      }
    }
  } else {
    // 损坏块是数据块或局部校验块
    std::vector<char> encode_result(repair_plan.block_size, 1);
    int num_of_blocks_involved = 0;
    for (auto &blocks_in_each_cluster : blocks) {
      num_of_blocks_involved += blocks_in_each_cluster.second.size();
    }

    std::vector<char *> data_v(num_of_blocks_involved);
    std::vector<char *> coding_v(1);
    char **data = (char **)data_v.data();
    char **coding = (char **)coding_v.data();

    int idx = 0;
    for (auto &num_of_blocks_involved : blocks) {
      for (auto &block : num_of_blocks_involved.second) {
        data[idx++] = block.second.data();
      }
    }
    coding[0] = encode_result.data();
    std::vector<int> new_matrix(1 * num_of_blocks_involved, 1);

    jerasure_matrix_encode(num_of_blocks_involved, 1, 8, new_matrix.data(),
                           data, coding, repair_plan.block_size);

    asio::write(peer, asio::buffer(encode_result, encode_result.size()));
  }

  asio::error_code ignore_ec;
  peer.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
  peer.close(ignore_ec);
}