list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules)
include(MinMaxOption)
include(CheckTypeSizeAlignment)

MinMaxOption(PARSEC_MAX_DATATYPE_NAME 64 64 256 "Maximum Datatype Name Length" TRUE)

parsec_check_type_size_alignment("short float" SHORT_FLOAT)
parsec_check_type_size_alignment(      "float"       FLOAT)
parsec_check_type_size_alignment(     "double"      DOUBLE)
parsec_check_type_size_alignment("long double" LONG_DOUBLE)

parsec_check_type_size_alignment("short float _Complex" SHORT_FLOAT__COMPLEX)
parsec_check_type_size_alignment(      "float _Complex"       FLOAT__COMPLEX)
parsec_check_type_size_alignment(     "double _Complex"      DOUBLE__COMPLEX)
parsec_check_type_size_alignment("long double _Complex" LONG_DOUBLE__COMPLEX)

parsec_check_type_size_alignment(    "_Bool"     _BOOL)
parsec_check_type_size_alignment(     "char"      CHAR)
parsec_check_type_size_alignment(    "short"     SHORT)
parsec_check_type_size_alignment(      "int"       INT)
parsec_check_type_size_alignment(     "long"      LONG)
parsec_check_type_size_alignment("long long" LONG_LONG)

parsec_check_type_size_alignment(  "int8_t"   INT8_T)
parsec_check_type_size_alignment( "int16_t"  INT16_T)
parsec_check_type_size_alignment( "int32_t"  INT32_T)
parsec_check_type_size_alignment( "int64_t"  INT64_T)
parsec_check_type_size_alignment("int128_t" INT128_T)

parsec_check_type_size_alignment(  "uint8_t"   UINT8_T)
parsec_check_type_size_alignment( "uint16_t"  UINT16_T)
parsec_check_type_size_alignment( "uint32_t"  UINT32_T)
parsec_check_type_size_alignment( "uint64_t"  UINT64_T)
parsec_check_type_size_alignment("uint128_t" UINT128_T)

parsec_check_type_size_alignment("wchar_t" WCHAR_T)

if(HAVE_SHORT_FLOAT)
  set(ALT_SHORT_FLOAT "short float" CACHE STRING "Alternate C 'short float' type if 'short float' is not available")
else()
  set(ALT_SHORT_FLOAT "_Float16"    CACHE STRING "Alternate C 'short float' type if 'short float' is not available")
endif()

if(ALT_SHORT_FLOAT)
  set(parsec_short_float_t         "${ALT_SHORT_FLOAT}")
  set(parsec_short_float_complex_t "${ALT_SHORT_FLOAT} _Complex")
  parsec_check_type_size_alignment(${parsec_short_float_t}         PARSEC_SHORT_FLOAT_T)
  parsec_check_type_size_alignment(${parsec_short_float_complex_t} PARSEC_SHORT_FLOAT_COMPLEX_T)
endif(ALT_SHORT_FLOAT)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/parsec_datatype_config.h.in
               ${CMAKE_CURRENT_BINARY_DIR}/parsec_datatype_config.h)

add_subdirectory(util)

set(DATATYPE_RELIABLE_SOURCES
  parsec_datatype_pack.c
  parsec_datatype_unpack.c
  util/crc.c
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
