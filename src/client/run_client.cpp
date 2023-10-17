#include "../../include/client.h"
#include "../../include/utils.h"
#include <cassert>
#include <cmath>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char **argv) {
  std::vector<std::string> args;
  for (int i = 0; i < argc; i++) {
    args.push_back(argv[i]);
  }

  assert(args.size() >= 8);

  EC_schema ec_schema;

  ec_schema.partial_decoding = (args[0] == "true");

  if (args[1] == "RS") {
    ec_schema.encode_type = Encode_Type::RS;
  } else if (args[1] == "OPPO_LRC") {
    ec_schema.encode_type = Encode_Type::OPPO_LRC;
  } else if (args[1] == "Azure_LRC") {
    ec_schema.encode_type = Encode_Type::Azure_LRC;
  } else if (args[1] == "Azure_LRC_1") {
    ec_schema.encode_type = Encode_Type::Azure_LRC_1;
  } else {
    assert(false);
  }

  Placement_Type placement_type;
  if (args[2] == "random") {
    ec_schema.placement_type = Placement_Type::random;
  } else if (args[2] == "flat") {
    ec_schema.placement_type = Placement_Type::flat;
  } else if (args[2] == "strategy1") {
    ec_schema.placement_type = Placement_Type::strategy1;
  } else {
    assert(false);
  }

  int value_length = -1;
  try {
    ec_schema.k = std::stoi(args[3]);
    ec_schema.real_l = std::stoi(args[4]);
    // 只考虑k被l整除的情况
    assert(ec_schema.k % ec_schema.real_l == 0);
    ec_schema.b = ec_schema.k / ec_schema.real_l;
    ec_schema.g = std::stoi(args[5]);
    ec_schema.strip_size_upper = std::stoi(args[6]);
    value_length = std::stoi(args[7]);
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }

  Client client;
  client.connect_to_coordinator();

  client.set_ec_parameter(ec_schema);

  std::unordered_map<std::string, std::string> key_value_s;
  auto [key, value] = generate_unique_random_strings(10, 1024 * 1024);
  key_value_s[key] = value;
  client.set(key, value);
  auto stored_value = client.get(key);
  assert(stored_value == value);

  return 0;
}