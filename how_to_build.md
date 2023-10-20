sudo apt-get install libprotobuf-dev protobuf-compiler libprotoc-dev

git clone https://github.com/ptbxzrt/LRC_memcached.git

cd ~/LRC_memcached/yalantinglibs
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
cmake --build .
cmake --install .

如果在本地单机上运行项目,相关参数
- coordinator的rpc端口号：11111