set(DATATYPE_RELIABLE_SOURCES
  parsec_datatype_pack.c
  parsec_datatype_unpack.c
)

if (NOT TARGET parsec-datatype-reliable)
  add_library(parsec-datatype-reliable OBJECT ${DATATYPE_RELIABLE_SOURCES})
endif (NOT TARGET parsec-datatype-reliable)

target_compile_definitions(parsec-datatype-reliable PRIVATE CHECKSUM)

set(DATATYPE_SOURCES
  parsec_convertor.c
  parsec_convertor_raw.c
  parsec_copy_functions.c
  parsec_copy_functions_heterogeneous.c
  parsec_datatype_add.c
  parsec_datatype_clone.c
  parsec_datatype_copy.c
  parsec_datatype_create.c
  parsec_datatype_create_contiguous.c
  parsec_datatype_destroy.c
  parsec_datatype_dump.c
  parsec_datatype_fake_stack.c
  parsec_datatype_get_count.c
  parsec_datatype_module.c
  parsec_datatype_monotonic.c
  parsec_datatype_optimize.c
  parsec_datatype_pack.c
  parsec_datatype_position.c
  parsec_datatype_resize.c
  parsec_datatype_unpack.c
)

if (NOT TARGET parsec-datatype)
  add_library(parsec-datatype OBJECT ${DATATYPE_SOURCES} $<TARGET_OBJECTS:parsec-datatype-reliable>)
endif (NOT TARGET parsec-datatype)
