cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(tarantool-avro)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" ${CMAKE_MODULE_PATH})

# Find Tarantool and Lua dependecies
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDE_DIRS})

# Set CFLAGS
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -fno-strict-aliasing -fvisibility=hidden")

add_library(avro_schema_rt_c SHARED avro_schema_rt.c)
set_target_properties(avro_schema_rt_c PROPERTIES PREFIX "" OUTPUT_NAME
                     "avro_schema_rt_c")

# Install module
install(FILES avro_schema.lua avro_schema_c.lua avro_schema_front.lua
              avro_schema_lir.lua avro_schema_rt.lua
        DESTINATION ${TARANTOOL_INSTALL_LUADIR})

install(TARGETS avro_schema_rt_c LIBRARY
        DESTINATION ${TARANTOOL_INSTALL_LIBDIR})

# enable_testing()
# add_test(avro tarantool ${CMAKE_SOURCE_DIR}/test/avro.test.lua)
# set_tests_properties(avro PROPERTIES ENVIRONMENT "BINARY_DIR=${PROJECT_BINARY_DIR}")
# add_custom_target(check COMMAND ctest -V)
add_custom_target(check COMMAND true)
