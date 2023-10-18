#include "../include/utils.h"

// 生成随机字符串
std::string generate_random_string(int length) {
  const std::string charset =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  const int charset_length = charset.length();

  std::mt19937 rng(std::time(0)); // 使用当前时间作为随机数种子
  std::uniform_int_distribution<int> distribution(0, charset_length - 1);

  std::string result;

  for (int i = 0; i < length; ++i) {
    int random_index = distribution(rng);
    result += charset[random_index];
  }

  return result;
}

// 生成不重复的随机字符串对
std::pair<std::string, std::string>
generate_unique_random_strings(int key_length, int value_length) {
  std::unordered_set<std::string> generated_keys;

  std::string key, value;

  do {
    key = generate_random_string(key_length);
  } while (!generated_keys.insert(key).second);

  value = generate_random_string(value_length);

  return std::make_pair(key, value);
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