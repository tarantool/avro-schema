include_directories(${CMAKE_SOURCE_DIR}/libavro/src)
add_library(avro SHARED avro.cpp)
target_link_libraries(avro avro-static)
set_target_properties(avro PROPERTIES PREFIX "" OUTPUT_NAME "avro")

# Install module
install(TARGETS avro LIBRARY DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/)
