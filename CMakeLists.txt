cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(tarantool-avro)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" ${CMAKE_MODULE_PATH})

# Find Tarantool and Lua dependecies
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDE_DIRS})

# Find MsgPuck llibrary
# set(MsgPuck_FIND_REQUIRED ON)
# find_package(MsgPuck)
# include_directories(${MSGPUCK_INCLUDE_DIRS})

# Find other dependecies

# Set CFLAGS
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -fvisibility=hidden")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fPIC -fvisibility=hidden -fvisibility-inlines-hidden")

# Build module
add_subdirectory(avro)
add_subdirectory(libavro)

# Add `make check
add_custom_target(check
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMAND env - BINARY_DIR=${PROJECT_BINARY_DIR}
            prove -v ./test/modulekit.test.lua)
