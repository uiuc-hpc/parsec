if (NOT TARGET parsec-datatype-util)
  add_library(parsec-datatype-util OBJECT crc.c arch.c)
endif (NOT TARGET parsec-datatype-util)
set_target_properties(parsec-datatype-util PROPERTIES POSITION_INDEPENDENT_CODE ${BUILD_SHARED_LIBS})
