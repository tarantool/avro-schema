cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(avro_schema)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" ${CMAKE_MODULE_PATH})

# Find Tarantool and Lua dependecies
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDE_DIRS})

# Check if __builtin_bswap16 is pesent
include(CheckCSourceCompiles)
check_c_source_compiles("int main() { return __builtin_bswap16(0); }" C_HAVE_BSWAP16)
if(C_HAVE_BSWAP16)
    add_definitions(-DC_HAVE_BSWAP16=1)
endif()

# Set CFLAGS/CXXFLAGS

# We need more warnings!
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra")

# MsgPack parser violates strict aliasing rules
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -fno-strict-aliasing")

# lib/phf is the only C++ user; it needs a bare minimum of C++ features
# Note: we use a fraction of libphf features and we hate dead cruft!
#       Also we'd like to avoid changing the source too much. We rely on
#       the linker to drop unused code and, since the entire libphf
#       comes in the single object file, we have to arrange for each
#       function to live in a separate section (-ffunction-sections).
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fno-rtti -fno-exceptions -fdata-sections -ffunction-sections")

# libphf doesn't really require libstdc++
add_definitions(-DPHF_NO_LIBCXX=1)

# libphf assumes UINT32_C macro definitions are available
add_definitions(-D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS)

# 1) list exported symbols explicitly, since only a fraction
#    of libphf is actually needed (--version-script)
# 2) enable linker to drop unused parts (--gc-sections)
# 3) don't link default libs, since libstdc++ is unnecessary
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
set (CMAKE_SHARED_LINKER_FLAGS
    "-Wl,-exported_symbols_list,${CMAKE_SOURCE_DIR}/exports_osx -Wl,-dead_strip -nodefaultlibs")
else()
set (CMAKE_SHARED_LINKER_FLAGS
    "-Wl,--version-script,${CMAKE_SOURCE_DIR}/exports -Wl,--gc-sections -nodefaultlibs")
endif()

add_library(avro_schema_rt_c SHARED
            runtime/pipeline.c
            runtime/hash.c
            runtime/misc.c
            lib/phf/phf.cc)
set_target_properties(avro_schema_rt_c PROPERTIES PREFIX "" OUTPUT_NAME
                     "avro_schema_rt_c" SUFFIX ".so" MACOSX_RPATH 0)

# link with libc explicitly (-nodefaultlibs earlier)
target_link_libraries(avro_schema_rt_c c)

# postprocess Lua file, replacing opcode.X named constants with values
add_custom_command(OUTPUT ${CMAKE_BINARY_DIR}/il_filt
                   DEPENDS avro_schema/il.lua
                   COMMAND ${CMAKE_SOURCE_DIR}/gen_il_filt.sh
                   ${CMAKE_SOURCE_DIR}/avro_schema/il.lua)

add_custom_command(OUTPUT ${CMAKE_BINARY_DIR}/il.lua
                   DEPENDS avro_schema/il.lua ${CMAKE_BINARY_DIR}/il_filt
                   COMMAND ${CMAKE_SOURCE_DIR}/il_filt.sh
                   ${CMAKE_SOURCE_DIR}/avro_schema/il.lua il.lua)

add_custom_command(OUTPUT ${CMAKE_BINARY_DIR}/backend.lua
                   DEPENDS avro_schema/backend.lua ${CMAKE_BINARY_DIR}/il_filt
                   COMMAND ${CMAKE_SOURCE_DIR}/il_filt.sh
                   ${CMAKE_SOURCE_DIR}/avro_schema/backend.lua backend.lua)

add_custom_target(postprocess_lua ALL DEPENDS
    ${CMAKE_BINARY_DIR}/il.lua
    ${CMAKE_BINARY_DIR}/backend.lua)

# Install module
install(FILES avro_schema/init.lua avro_schema/compiler.lua
              avro_schema/frontend.lua avro_schema/runtime.lua
        DESTINATION ${TARANTOOL_INSTALL_LUADIR}/avro_schema)

install(FILES ${CMAKE_BINARY_DIR}/il.lua
        DESTINATION ${TARANTOOL_INSTALL_LUADIR}/avro_schema)

install(FILES ${CMAKE_BINARY_DIR}/backend.lua
        DESTINATION ${TARANTOOL_INSTALL_LUADIR}/avro_schema)

install(TARGETS avro_schema_rt_c LIBRARY
        DESTINATION ${TARANTOOL_INSTALL_LIBDIR})

# testing

set(TARANTOOL tarantool)

set(LUA_PATH
    "${CMAKE_SOURCE_DIR}/?.lua\\;${CMAKE_SOURCE_DIR}/?/init.lua\\;\\;")

set(LUA_CPATH "${CMAKE_BINARY_DIR}/?.so\\;\\;")

set(TERRA_PATH "${CMAKE_SOURCE_DIR}/?.t\\;\\;")

enable_testing()

add_test(NAME ddt_tests
         COMMAND ${TARANTOOL} ${CMAKE_SOURCE_DIR}/test/run_ddt_tests.lua
         WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)

add_test(NAME api_tests
         COMMAND ${TARANTOOL} ${CMAKE_SOURCE_DIR}/test/api_tests.lua
         WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)

add_test(NAME buf_grow_test
         COMMAND ${TARANTOOL} ${CMAKE_SOURCE_DIR}/test/buf_grow_test.lua
         WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)

set(TESTS ddt_tests api_tests buf_grow_test)
foreach(test IN LISTS TESTS)

    set_property(TEST ${test} PROPERTY ENVIRONMENT "LUA_PATH=${LUA_PATH}")

    set_property(TEST ${test} APPEND PROPERTY ENVIRONMENT
                 "LUA_CPATH=${LUA_CPATH}")

    set_property(TEST ${test} APPEND PROPERTY ENVIRONMENT
                 "TERRA_PATH=${TERRA_PATH}")

endforeach()

add_custom_target(check COMMAND ctest -V)

add_custom_target(benchmark
                  COMMAND env "LUA_PATH=${LUA_PATH}"
                              "LUA_CPATH=${LUA_CPATH}"
                              "TERRA_PATH=${TERRA_PATH}"
                          ${TARANTOOL} ${CMAKE_SOURCE_DIR}/benchmark.lua)

configure_file(setup_env.template.sh setup_env.sh)
