cmake_minimum_required(VERSION 3.13)

project(mini-kafka)

set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard

# Collect all source files
file(GLOB_RECURSE SOURCE_FILES 
    src/api/*.cpp 
    src/core/*.cpp 
    src/protocol/*.cpp 
    src/storage/*.cpp
)

# Collect all header files
file(GLOB_RECURSE HEADER_FILES 
    src/api/*.hpp 
    src/core/*.hpp 
    src/protocol/*.hpp 
    src/storage/*.hpp
)

add_executable(kafka 
    ${SOURCE_FILES}
    ${HEADER_FILES}
)

target_include_directories(kafka PUBLIC src)

# Link pthread for threading support
find_package(Threads REQUIRED)
target_link_libraries(kafka Threads::Threads)