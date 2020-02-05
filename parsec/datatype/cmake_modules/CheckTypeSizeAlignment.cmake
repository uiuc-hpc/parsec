include(CheckTypeSize)

get_filename_component(__check_type_size_alignment_dir "${CMAKE_CURRENT_LIST_FILE}" DIRECTORY)

function(__check_type_alignment_impl type var)
  if(NOT CMAKE_REQUIRED_QUIET)
    message(STATUS "Check alignment of ${type}")
  endif()

  set(headers)
  if(HAVE_SYS_TYPES_H)
    string(APPEND headers "#include <sys/types.h>\n")
  endif()
  if(HAVE_STDINT_H)
    string(APPEND headers "#include <stdint.h>\n")
  endif()
  if(HAVE_STDDEF_H)
    string(APPEND headers "#include <stddef.h>\n")
  endif()
  foreach(h ${CMAKE_EXTRA_INCLUDE_FILES})
    string(APPEND headers "#include \"${h}\"\n")
  endforeach()

  set(src ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CheckTypeSizeAlignment/${var}.c)
  set(bin ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CheckTypeSizeAlignment/${var}.bin)
  configure_file(${__check_type_size_alignment_dir}/CheckTypeSizeAlignment.c.in ${src} @ONLY)

  try_compile(HAVE_ALIGNOF_${var} ${CMAKE_BINARY_DIR} ${src}
    COMPILE_DEFINITIONS ${CMAKE_REQUIRED_DEFINITIONS}
    LINK_OPTIONS ${CMAKE_REQUIRED_LINK_OPTIONS}
    LINK_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES}
    CMAKE_FLAGS
      "-DCOMPILE_DEFINITIONS:STRING=${CMAKE_REQUIRED_FLAGS}"
      "-DINCLUDE_DIRECTORIES:STRING=${CMAKE_REQUIRED_INCLUDES}"
    OUTPUT_VARIABLE output
    COPY_FILE ${bin}
    )

  if(HAVE_ALIGNOF_${var})
    # The check compiled.  Load information from the binary.
    file(STRINGS ${bin} string LIMIT_COUNT 1 REGEX "INFO:alignment")

    # Parse the information string.
    set(regex_alignment ".*INFO:alignment\\[0*([^]]*)\\].*")
    if("${string}" MATCHES "${regex_alignment}")
      set(ALIGNOF_${var} "${CMAKE_MATCH_1}")
    endif()

    if(NOT CMAKE_REQUIRED_QUIET)
      message(STATUS "Check alignment of ${type} - done")
    endif()
    file(APPEND ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeOutput.log
      "Determining alignment of ${type} passed with the following output:\n${output}\n\n")
    set(ALIGNOF_${var} "${ALIGNOF_${var}}" CACHE INTERNAL "CHECK_TYPE_ALIGNMENT: alignof(${type})")
  else()
    # The check failed to compile.
    if(NOT CMAKE_REQUIRED_QUIET)
      message(STATUS "Check alignment of ${type} - failed")
    endif()
    file(READ ${src} content)
    file(APPEND ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeError.log
      "Determining alignment of ${type} failed with the following output:\n${output}\n${src}:\n${content}\n\n")
    set(ALIGNOF_${var} "" CACHE INTERNAL "CHECK_TYPE_ALIGNMENT: ${type} unknown")
    file(REMOVE ${map})
  endif()
endfunction()

macro(CHECK_TYPE_SIZE_ALIGNMENT TYPE VARIABLE)
  check_type_size(${TYPE} ${VARIABLE})
  if(HAVE_${VARIABLE} AND NOT DEFINED HAVE_ALIGNOF_${VARIABLE})
      __check_type_alignment_impl(${TYPE} ${VARIABLE})
  endif()
endmacro()

macro(PARSEC_CHECK_TYPE_SIZE_ALIGNMENT TYPE VARIABLE)
  check_type_size_alignment(${TYPE} ${VARIABLE})
  if(HAVE_${VARIABLE})
    set(SIZEOF_${VARIABLE} "${${VARIABLE}}")
  endif()
endmacro()
