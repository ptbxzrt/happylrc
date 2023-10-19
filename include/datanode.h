#pragma once

#include "asio.hpp"
#include "utils.h"
#include <string>
#include <sw/redis++/redis++.h>

class Datanode {
public:
  Datanode(std::string ip, int port);
  ~Datanode();
  void keep_working();

private:
  std::string ip_;
  int port_;
  asio::io_context io_context_{};
  asio::ip::tcp::acceptor acceptor_;
  std::unique_ptr<sw::redis::Redis> redis_{nullptr};
};