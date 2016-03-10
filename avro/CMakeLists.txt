include_directories(${CMAKE_SOURCE_DIR}/libavro/src)
add_library(avro SHARED avro.cpp)
target_link_libraries(avro ${MSGPUCK_LIBRARIES} avro-static)
set_target_properties(avro PROPERTIES PREFIX "" OUTPUT_NAME "avro")
# unit test loads avrotest, not avro
# effectively invoking alternative entry point that exposes a few
# additional routines
add_custom_command(
	TARGET avro PRE_LINK
	COMMAND cmake -E create_symlink avrotest.so avro.so)

# Install module
install(TARGETS avro LIBRARY DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/)
