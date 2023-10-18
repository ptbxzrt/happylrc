cmake_minimum_required(VERSION 3.15)
project(memlrc)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

# 设置头文件搜索路径
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/3rd_party/yalantinglibs/include
    ${CMAKE_CURRENT_SOURCE_DIR}/3rd_party/gf-complete/include
    ${CMAKE_CURRENT_SOURCE_DIR}/3rd_party/Jerasure/include/jerasure
    ${CMAKE_CURRENT_SOURCE_DIR}/3rd_party/Jerasure/include
    ${CMAKE_CURRENT_SOURCE_DIR}/3rd_party/hiredis/include
    ${CMAKE_CURRENT_SOURCE_DIR}/3rd_party/redis-plus-plus/include
)

# 设置库文件搜索路径
link_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/3rd_party/Jerasure/lib
    ${CMAKE_CURRENT_SOURCE_DIR}/3rd_party/gf-complete/lib
    ${CMAKE_CURRENT_SOURCE_DIR}/3rd_party/hiredis/lib
    ${CMAKE_CURRENT_SOURCE_DIR}/3rd_party/redis-plus-plus/lib
)

# 共享的源文件
set(SHARED_SOURCES
    src/utils.cpp
    src/erasure_code.cpp
    src/tinyxml2.cpp
)

add_executable(run_client
    src/client/client.cpp
    src/client/run_client.cpp
    ${SHARED_SOURCES}
)

add_executable(run_coordinator
    src/coordinator/coordinator.cpp
    src/coordinator/run_coordinator.cpp
    ${SHARED_SOURCES}
)

add_executable(run_proxy
    src/proxy/proxy.cpp
    src/proxy/run_proxy.cpp
    ${SHARED_SOURCES}
)

add_executable(run_datanode
    src/datanode/datanode.cpp
    src/datanode/run_datanode.cpp
    ${SHARED_SOURCES}
)

target_link_libraries(run_client Threads::Threads Jerasure gf_complete redis++)
target_link_libraries(run_coordinator Threads::Threads Jerasure gf_complete redis++)
target_link_libraries(run_proxy Threads::Threads Jerasure gf_complete redis++)
target_link_libraries(run_datanode Threads::Threads Jerasure gf_complete redis++)