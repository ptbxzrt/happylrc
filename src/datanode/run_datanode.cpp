#include "../../include/datanode.h"

void daemonize() {
  // 1. 创建子进程,然后终止父进程
  pid_t pid = fork();
  if (pid < 0) {
    std::cerr << "Fork failed" << std::endl;
    exit(1);
  }
  if (pid > 0) {
    exit(0); // 终止父进程
  }

  // 2. 在子进程中创建新会话
  if (setsid() < 0) {
    std::cerr << "setsid failed" << std::endl;
    exit(1);
  }

  // 3. 改变当前工作目录到根目录
  if (chdir("/") < 0) {
    std::cerr << "chdir failed" << std::endl;
    exit(1);
  }

  // 4. 关闭标准输入、标准输出、标准错误流
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);

  // 5. 重定向标准输入、标准输出、标准错误流到/dev/null或其他文件
  int null_fd = open("/dev/null", O_RDWR);
  dup2(null_fd, STDIN_FILENO);
  dup2(null_fd, STDOUT_FILENO);
  dup2(null_fd, STDERR_FILENO);
  close(null_fd);
}

int main(int argc, char **argv) {
  // 6. 将程序修改为守护进程
  daemonize();

  // 7. 在守护进程中执行你的程序
  my_assert(argc == 3);

  std::string ip = argv[1];
  std::string port = argv[2];
  Datanode datanode(ip, std::stoi(port));
  datanode.keep_working();

  return 0;
}