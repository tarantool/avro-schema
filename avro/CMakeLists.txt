add_library(cfunctions SHARED cfunctions.c)
target_link_libraries(cfunctions ${MSGPUCK_LIBRARIES})
set_target_properties(cfunctions PROPERTIES PREFIX "" OUTPUT_NAME "cfunctions")

# Install module
install(FILES init.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR}/${PROJECT_NAME}/)
install(FILES functions.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR}/${PROJECT_NAME}/)
install(TARGETS cfunctions LIBRARY DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/${PROJECT_NAME}/)
