cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(tarantool-avro)

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
set (CMAKE_SHARED_LINKER_FLAGS
    "-Wl,--version-script,${CMAKE_SOURCE_DIR}/exports -Wl,--gc-sections -nodefaultlibs")

add_library(avro_schema_rt_c SHARED avro_schema_rt.c lib/phf/phf.cc)
set_target_properties(avro_schema_rt_c PROPERTIES PREFIX "" OUTPUT_NAME
                     "avro_schema_rt_c")

# link with libc explicitly (-nodefaultlibs earlier)
target_link_libraries(avro_schema_rt_c c)

# postprocess Lua file, replacing opcode.X named constants with values
add_custom_command(OUTPUT ${CMAKE_BINARY_DIR}/il_filt
                   DEPENDS avro_schema_il.lua
                   COMMAND ${CMAKE_SOURCE_DIR}/gen_il_filt.sh
                   ${CMAKE_SOURCE_DIR}/avro_schema_il.lua)

add_custom_command(OUTPUT ${CMAKE_BINARY_DIR}/avro_schema_il_.lua
                   DEPENDS avro_schema_il.lua ${CMAKE_BINARY_DIR}/il_filt
                   COMMAND ${CMAKE_SOURCE_DIR}/il_filt.sh
                   ${CMAKE_SOURCE_DIR}/avro_schema_il.lua avro_schema_il_.lua)

add_custom_command(OUTPUT ${CMAKE_BINARY_DIR}/avro_schema_backend_lua_.lua
                   DEPENDS avro_schema_backend_lua.lua ${CMAKE_BINARY_DIR}/il_filt
                   COMMAND ${CMAKE_SOURCE_DIR}/il_filt.sh
                   ${CMAKE_SOURCE_DIR}/avro_schema_backend_lua.lua
                   avro_schema_backend_lua_.lua)

add_custom_target(postprocess_lua ALL DEPENDS
    ${CMAKE_BINARY_DIR}/avro_schema_il_.lua
    ${CMAKE_BINARY_DIR}/avro_schema_backend_lua_.lua)

# Install module
install(FILES avro_schema.lua avro_schema_c.lua avro_schema_front.lua
              avro_schema_rt.lua
        DESTINATION ${TARANTOOL_INSTALL_LUADIR})

install(FILES ${CMAKE_BINARY_DIR}/avro_schema_il_.lua RENAME avro_schema_il.lua
        DESTINATION ${TARANTOOL_INSTALL_LUADIR})

install(FILES ${CMAKE_BINARY_DIR}/avro_schema_backend_lua_.lua
        RENAME avro_schema_backend_lua.lua
        DESTINATION ${TARANTOOL_INSTALL_LUADIR})

install(TARGETS avro_schema_rt_c LIBRARY
        DESTINATION ${TARANTOOL_INSTALL_LIBDIR})

# enable_testing()
# add_test(avro tarantool ${CMAKE_SOURCE_DIR}/test/avro.test.lua)
# set_tests_properties(avro PROPERTIES ENVIRONMENT "BINARY_DIR=${PROJECT_BINARY_DIR}")
# add_custom_target(check COMMAND ctest -V)
add_custom_target(check COMMAND true)
