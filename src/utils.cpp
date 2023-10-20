#include "../include/utils.h"

const std::string charset =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

// 生成随机字符串
std::string
generate_random_string(int length,
                       std::uniform_int_distribution<int> &distribution,
                       std::mt19937 &rng) {
  std::string result;

  for (int i = 0; i < length; ++i) {
    int random_index = distribution(rng);
    result += charset[random_index];
  }

  return result;
}

// 生成若干不重复的随机字符串对
void generate_unique_random_strings(
    int key_length, int value_length, int n,
    std::unordered_map<std::string, std::string> &key_value) {

  std::mt19937 rng(std::time(0)); // 使用当前时间作为随机数种子
  std::uniform_int_distribution<int> distribution(0, charset.size() - 1);

  for (int i = 0; i < n; i++) {
    std::string key;

    do {
      key = generate_random_string(key_length, distribution, rng);
    } while (key_value.contains(key) == true);

    std::string value(value_length, key[0]);

    key_value[key] = value;
  }
}

int random_index(size_t len) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dist(0, len - 1);
  return dist(gen);
}

void exit_when(bool condition, const std::source_location &location) {
  if (!condition) {
    std::cerr << "条件失败于 " << location.file_name() << ":" << location.line()
              << " - " << location.function_name() << std::endl;
    std::exit(EXIT_FAILURE);
  }
}

int bytes_to_int(std::vector<unsigned char> &bytes) {
  int integer;
  unsigned char *p = (unsigned char *)(&integer);
  for (int i = 0; i < int(bytes.size()); i++) {
    memcpy(p + i, &bytes[i], 1);
  }
  return integer;
}

std::vector<unsigned char> int_to_bytes(int integer) {
  std::vector<unsigned char> bytes(sizeof(int));
  unsigned char *p = (unsigned char *)(&integer);
  for (int i = 0; i < int(bytes.size()); i++) {
    memcpy(&bytes[i], p + i, 1);
  }
  return bytes;
}