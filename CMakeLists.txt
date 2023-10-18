cmake_minimum_required(VERSION 3.15)
project(memlrc)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_INCLUDE_CURRENT_DIR ON)
find_package(Threads REQUIRED)
link_libraries(Threads::Threads)
find_package(yalantinglibs REQUIRED)
link_libraries(yalantinglibs::yalantinglibs)

add_executable(run_client
    include/client.h
    src/client/client.cpp
    src/client/run_client.cpp
    src/utils.cpp)

add_executable(run_coordinator
    include/coordinator.h
    src/coordinator/coordinator.cpp
    src/coordinator/run_coordinator.cpp
    src/utils.cpp)

add_executable(run_proxy
    include/proxy.h
    src/proxy/proxy.cpp
    src/proxy/run_proxy.cpp
    src/utils.cpp)