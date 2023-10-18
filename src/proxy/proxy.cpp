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

  rpc_coordinator_ = std::make_unique<coro_rpc::coro_rpc_client>();
  async_simple::coro::syncAwait(rpc_coordinator_->connect(
      coordinator_ip_, std::to_string(coordinator_port_)));
}

Proxy::~Proxy() { rpc_server_->stop(); }

void Proxy::start() { auto err = rpc_server_->start(); }

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
        writers.push_back(std::thread([this, j, k, block_id, data, coding,
                                       cur_block_size,
                                       ip_and_port_of_datanode]() {
          if (j < k) {
            write_to_redis_or_memcached(block_id.c_str(), block_id.size(),
                                        data[j], cur_block_size,
                                        ip_and_port_of_datanode.first.c_str(),
                                        ip_and_port_of_datanode.second);
          } else {
            write_to_redis_or_memcached(block_id.c_str(), block_id.size(),
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
        readers.push_back(std::thread([this, j, stripe_id, blocks_ptr,
                                       cur_block_size,
                                       ip_and_port_of_datanode]() {
          std::string block_id = std::to_string(stripe_id * 1000 + j);
          std::string block(cur_block_size, 0);
          read_from_redis_or_memcached(block_id.c_str(), block_id.size(),
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

void Proxy::write_to_redis_or_memcached(const char *key, size_t key_len,
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

void Proxy::read_from_redis_or_memcached(const char *key, size_t key_len,
                                         char *value, size_t value_len,
                                         const char *ip, int port) {
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