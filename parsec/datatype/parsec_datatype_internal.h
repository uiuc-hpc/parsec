/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2019 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _PARSEC_DATATYPE_INTERNAL_H_HAS_BEEN_INCLUDED
#define _PARSEC_DATATYPE_INTERNAL_H_HAS_BEEN_INCLUDED

#include "parsec/parsec_config.h"
#include "parsec/datatype/parsec_datatype_config.h"

#include <stdarg.h>
#include <string.h>

#if defined(VERBOSE)
#include "parsec/utils/output.h"

extern int parsec_datatype_dfd;

#  define DDT_DUMP_STACK( PSTACK, STACK_POS, PDESC, NAME ) \
     parsec_datatype_dump_stack( (PSTACK), (STACK_POS), (PDESC), (NAME) )
#  define DUMP( ... )          parsec_output(parsec_datatype_dfd, __VA_ARGS__)
#else
#  define DDT_DUMP_STACK( PSTACK, STACK_POS, PDESC, NAME )
#  define DUMP( ... )
#endif  /* VERBOSE */


/*
 * There 3 types of predefined data types.
 * - the basic one composed by just one basic datatype which are
 *   definitively contiguous
 * - the derived ones where the same basic type is used multiple times.
 *   They should be most of the time contiguous.
 * - and finally the derived one where multiple basic types are used.
 *   Depending on the architecture they can be contiguous or not.
 *
 * At the PARSEC-level we do not care from which language the datatype came from
 * (C, C++ or FORTRAN), we only focus on their internal representation in
 * the host memory.
 *
 * NOTE: This predefined datatype order should be matched by any upper-level
 * users of the PARSEC datatype.
 */
#define PARSEC_DATATYPE_LOOP           0
#define PARSEC_DATATYPE_END_LOOP       1
#define PARSEC_DATATYPE_LB             2
#define PARSEC_DATATYPE_UB             3
#define PARSEC_DATATYPE_FIRST_TYPE     4 /* Number of first real type */
#define PARSEC_DATATYPE_INT1           4
#define PARSEC_DATATYPE_INT2           5
#define PARSEC_DATATYPE_INT4           6
#define PARSEC_DATATYPE_INT8           7
#define PARSEC_DATATYPE_INT16          8
#define PARSEC_DATATYPE_UINT1          9
#define PARSEC_DATATYPE_UINT2          10
#define PARSEC_DATATYPE_UINT4          11
#define PARSEC_DATATYPE_UINT8          12
#define PARSEC_DATATYPE_UINT16         13
#define PARSEC_DATATYPE_FLOAT2         14
#define PARSEC_DATATYPE_FLOAT4         15
#define PARSEC_DATATYPE_FLOAT8         16
#define PARSEC_DATATYPE_FLOAT12        17
#define PARSEC_DATATYPE_FLOAT16        18
#define PARSEC_DATATYPE_SHORT_FLOAT_COMPLEX 19
#define PARSEC_DATATYPE_FLOAT_COMPLEX  20
#define PARSEC_DATATYPE_DOUBLE_COMPLEX 21
#define PARSEC_DATATYPE_LONG_DOUBLE_COMPLEX 22
#define PARSEC_DATATYPE_BOOL           23
#define PARSEC_DATATYPE_WCHAR          24
#define PARSEC_DATATYPE_UNAVAILABLE    25

#ifndef PARSEC_DATATYPE_MAX_PREDEFINED
#define PARSEC_DATATYPE_MAX_PREDEFINED (PARSEC_DATATYPE_UNAVAILABLE+1)
#elif PARSEC_DATATYPE_MAX_PREDEFINED <= PARSEC_DATATYPE_UNAVAILABLE
/*
 * If the number of basic datatype should change update
 * PARSEC_DATATYPE_MAX_PREDEFINED in parsec_datatype.h
 */
#error PARSEC_DATATYPE_MAX_PREDEFINED should be updated to the next value after the PARSEC_DATATYPE_UNAVAILABLE define
#endif

#define DT_INCREASE_STACK     8

BEGIN_C_DECLS

struct ddt_elem_id_description {
    uint16_t   flags;  /**< flags for the record */
    uint16_t   type;   /**< the basic data type id */
};
typedef struct ddt_elem_id_description ddt_elem_id_description;

/**
 * The data element description. It is similar to a vector type, a contiguous
 * blocklen number of basic elements, with a displacement for the first element
 * and then an extent for all the extra count.
 */
struct ddt_elem_desc {
    ddt_elem_id_description common;           /**< basic data description and flags */
    uint32_t                count;            /**< number of blocks */
    size_t                  blocklen;         /**< number of elements on each block */
    ptrdiff_t               extent;           /**< extent of each block (in bytes) */
    ptrdiff_t               disp;             /**< displacement of the first block */
};
typedef struct ddt_elem_desc ddt_elem_desc_t;

/**
 * The loop description, with it's two markers: one for the begining and one for
 * the end. The initial marker contains the number of repetitions, the number of
 * elements in the loop, and the extent of each loop. The end marker contains in
 * addition to the number of elements (so that we can easily pair together the
 * two markers), the size of the data contained inside and the displacement of
 * the first element.
 */
struct ddt_loop_desc {
    ddt_elem_id_description common;           /**< basic data description and flags */
    uint32_t                items;            /**< number of items in the loop */
    uint32_t                loops;            /**< number of elements */
    size_t                  unused;           /**< not used right now */
    ptrdiff_t               extent;           /**< extent of the whole loop */
};
typedef struct ddt_loop_desc ddt_loop_desc_t;

struct ddt_endloop_desc {
    ddt_elem_id_description common;           /**< basic data description and flags */
    uint32_t                items;            /**< number of elements */
    uint32_t                unused;           /**< not used right now */
    size_t                  size;             /**< real size of the data in the loop */
    ptrdiff_t               first_elem_disp;  /**< the displacement of the first block in the loop */
};
typedef struct ddt_endloop_desc ddt_endloop_desc_t;

union dt_elem_desc {
    ddt_elem_desc_t    elem;
    ddt_loop_desc_t    loop;
    ddt_endloop_desc_t end_loop;
};

#define CREATE_LOOP_START( _place, _count, _items, _extent, _flags )           \
    do {                                                                       \
        (_place)->loop.common.type   = PARSEC_DATATYPE_LOOP;                     \
        (_place)->loop.common.flags  = (_flags) & ~PARSEC_DATATYPE_FLAG_DATA;    \
        (_place)->loop.loops         = (_count);                               \
        (_place)->loop.items         = (_items);                               \
        (_place)->loop.extent        = (_extent);                              \
        (_place)->loop.unused        = -1;                                     \
    } while(0)

#define CREATE_LOOP_END( _place, _items, _first_item_disp, _size, _flags )     \
    do {                                                                       \
        (_place)->end_loop.common.type = PARSEC_DATATYPE_END_LOOP;               \
        (_place)->end_loop.common.flags = (_flags) & ~PARSEC_DATATYPE_FLAG_DATA; \
        (_place)->end_loop.items = (_items);                                   \
        (_place)->end_loop.first_elem_disp = (_first_item_disp);               \
        (_place)->end_loop.size = (_size);  /* the size inside the loop */     \
        (_place)->end_loop.unused = -1;                                        \
    } while(0)


/**
 * Create an element entry in the description. If the element is contiguous
 * collapse everything into the blocklen.
 */
#define CREATE_ELEM(_place, _type, _flags, _blocklen, _count, _disp, _extent)  \
    do {                                                                       \
        (_place)->elem.common.flags = (_flags) | PARSEC_DATATYPE_FLAG_DATA;      \
        (_place)->elem.common.type  = (_type);                                 \
        (_place)->elem.blocklen     = (_blocklen);                             \
        (_place)->elem.count        = (_count);                                \
        (_place)->elem.extent       = (_extent);                               \
        (_place)->elem.disp         = (_disp);                                 \
        if( _extent == (ptrdiff_t)(_blocklen * parsec_datatype_basicDatatypes[_type]->size) ) { \
            /* collapse it into a single large blocklen */              \
            (_place)->elem.blocklen *= _count;                          \
            (_place)->elem.extent   *= _count;                          \
            (_place)->elem.count     = 1;                               \
        }                                                               \
    } while(0)
/*
 * This array holds the descriptions desc.desc[2] of the predefined basic datatypes.
 */
PARSEC_DECLSPEC extern union dt_elem_desc parsec_datatype_predefined_elem_desc[2 * PARSEC_DATATYPE_MAX_PREDEFINED];
struct parsec_datatype_s;

/* Other fields starting after bdt_used (index of PARSEC_DATATYPE_LOOP should be ONE) */
/*
 * NOTE: The order of initialization *MUST* match the order of the PARSEC_DATATYPE_-numbers.
 * Unfortunateley, I don't get the preprocessor to replace
 *     PARSEC_DATATYPE_INIT_BTYPES_ARRAY_ ## PARSEC_DATATYPE ## NAME
 * into
 *     PARSEC_DATATYPE_INIT_BTYPES_ARRAY_[0-21], then order and naming would _not_ matter....
 */

#define PARSEC_DATATYPE_INIT_PTYPES_ARRAY_UNAVAILABLE NULL
#define PARSEC_DATATYPE_INIT_PTYPES_ARRAY(NAME) (size_t[PARSEC_DATATYPE_MAX_PREDEFINED]){ [PARSEC_DATATYPE_ ## NAME] = 1, [PARSEC_DATATYPE_MAX_PREDEFINED-1] = 0 }

#define PARSEC_DATATYPE_INIT_NAME(NAME) "PARSEC_" #NAME

/*
 * Macro to initialize the main description for basic types, setting the pointer
 * into the array parsec_datatype_predefined_type_desc, which is initialized at
 * runtime in parsec_datatype_init(). Each basic type has two desc-elements....
 */
#define PARSEC_DATATYPE_INIT_DESC_PREDEFINED(NAME)                                     \
    {                                                                                \
        .length = 1, .used = 1,                                                      \
        .desc = &(parsec_datatype_predefined_elem_desc[2 * PARSEC_DATATYPE_ ## NAME])    \
    }
#define PARSEC_DATATYPE_INIT_DESC_NULL  {.length = 0, .used = 0, .desc = NULL}

#define PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( NAME, FLAGS )                   \
    {                                                                                \
        .super = PARSEC_OBJ_STATIC_INIT(parsec_datatype_s),                              \
        .flags = PARSEC_DATATYPE_FLAG_UNAVAILABLE | PARSEC_DATATYPE_FLAG_PREDEFINED | (FLAGS), \
        .id = PARSEC_DATATYPE_ ## NAME,                                                \
        .bdt_used = 0,                                                               \
        .size = 0,                                                                   \
        .true_lb = 0, .true_ub = 0, .lb = 0, .ub = 0,                                \
        .align = 0,                                                                  \
        .nbElems = 1,                                                                \
        .name = PARSEC_DATATYPE_INIT_NAME(NAME),                                       \
        .desc = PARSEC_DATATYPE_INIT_DESC_PREDEFINED(UNAVAILABLE),                     \
        .opt_desc = PARSEC_DATATYPE_INIT_DESC_PREDEFINED(UNAVAILABLE),                 \
        .ptypes = PARSEC_DATATYPE_INIT_PTYPES_ARRAY_UNAVAILABLE                        \
    }

#define PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE( FLAGS )                               \
    PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( UNAVAILABLE, (FLAGS) )

#define PARSEC_DATATYPE_INITIALIZER_EMPTY( FLAGS )                        \
    {                                                                   \
        .super = PARSEC_OBJ_STATIC_INIT(parsec_datatype_s),                 \
        .flags = PARSEC_DATATYPE_FLAG_PREDEFINED | (FLAGS),               \
        .id = 0,                                                        \
        .bdt_used = 0,                                                  \
        .size = 0,                                                      \
        .true_lb = 0, .true_ub = 0, .lb = 0, .ub = 0,                   \
        .align = 0,                                                     \
        .nbElems = 1,                                                   \
        .name = PARSEC_DATATYPE_INIT_NAME(EMPTY),                         \
        .desc = PARSEC_DATATYPE_INIT_DESC_NULL,                           \
        .opt_desc = PARSEC_DATATYPE_INIT_DESC_NULL,                       \
        .ptypes = PARSEC_DATATYPE_INIT_PTYPES_ARRAY_UNAVAILABLE           \
    }

#define PARSEC_DATATYPE_INIT_BASIC_TYPE( TYPE, NAME, FLAGS )              \
    {                                                                   \
        .super = PARSEC_OBJ_STATIC_INIT(parsec_datatype_s),                 \
        .flags = PARSEC_DATATYPE_FLAG_PREDEFINED | (FLAGS),               \
        .id = TYPE,                                                     \
        .bdt_used = (((uint32_t)1)<<(TYPE)),                            \
        .size = 0,                                                      \
        .true_lb = 0, .true_ub = 0, .lb = 0, .ub = 0,                   \
        .align = 0,                                                     \
        .nbElems = 1,                                                   \
        .name = PARSEC_DATATYPE_INIT_NAME(NAME),                          \
        .desc = PARSEC_DATATYPE_INIT_DESC_NULL,                           \
        .opt_desc = PARSEC_DATATYPE_INIT_DESC_NULL,                       \
        .ptypes = PARSEC_DATATYPE_INIT_PTYPES_ARRAY_UNAVAILABLE           \
    }

#define PARSEC_DATATYPE_INIT_BASIC_DATATYPE( TYPE, ALIGN, NAME, FLAGS )                \
    {                                                                                \
        .super = PARSEC_OBJ_STATIC_INIT(parsec_datatype_s),                              \
        .flags = PARSEC_DATATYPE_FLAG_BASIC | (FLAGS),                                 \
        .id = PARSEC_DATATYPE_ ## NAME,                                                \
        .bdt_used = (((uint32_t)1)<<(PARSEC_DATATYPE_ ## NAME)),                       \
        .size = sizeof(TYPE),                                                        \
        .true_lb = 0, .true_ub = sizeof(TYPE), .lb = 0, .ub = sizeof(TYPE),          \
        .align = (ALIGN),                                                            \
        .nbElems = 1,                                                                \
        .name = PARSEC_DATATYPE_INIT_NAME(NAME),                                       \
        .desc = PARSEC_DATATYPE_INIT_DESC_PREDEFINED(NAME),                            \
        .opt_desc = PARSEC_DATATYPE_INIT_DESC_PREDEFINED(NAME),                        \
        .ptypes = PARSEC_DATATYPE_INIT_PTYPES_ARRAY_UNAVAILABLE                        \
    }

#define PARSEC_DATATYPE_INITIALIZER_LOOP(FLAGS)       PARSEC_DATATYPE_INIT_BASIC_TYPE( PARSEC_DATATYPE_LOOP, LOOP_S, FLAGS )
#define PARSEC_DATATYPE_INITIALIZER_END_LOOP(FLAGS)   PARSEC_DATATYPE_INIT_BASIC_TYPE( PARSEC_DATATYPE_END_LOOP, LOOP_E, FLAGS )
#define PARSEC_DATATYPE_INITIALIZER_LB(FLAGS)         PARSEC_DATATYPE_INIT_BASIC_TYPE( PARSEC_DATATYPE_LB, LB, FLAGS )
#define PARSEC_DATATYPE_INITIALIZER_UB(FLAGS)         PARSEC_DATATYPE_INIT_BASIC_TYPE( PARSEC_DATATYPE_UB, UB, FLAGS )
#define PARSEC_DATATYPE_INITIALIZER_INT1(FLAGS)       PARSEC_DATATYPE_INIT_BASIC_DATATYPE( int8_t, ALIGNOF_INT8_T, INT1, FLAGS )
#define PARSEC_DATATYPE_INITIALIZER_INT2(FLAGS)       PARSEC_DATATYPE_INIT_BASIC_DATATYPE( int16_t, ALIGNOF_INT16_T, INT2, FLAGS )
#define PARSEC_DATATYPE_INITIALIZER_INT4(FLAGS)       PARSEC_DATATYPE_INIT_BASIC_DATATYPE( int32_t, ALIGNOF_INT32_T, INT4, FLAGS )
#define PARSEC_DATATYPE_INITIALIZER_INT8(FLAGS)       PARSEC_DATATYPE_INIT_BASIC_DATATYPE( int64_t, ALIGNOF_INT64_T, INT8, FLAGS )
#ifdef HAVE_INT128_T
#define PARSEC_DATATYPE_INITIALIZER_INT16(FLAGS)      PARSEC_DATATYPE_INIT_BASIC_DATATYPE( int128_t, ALIGNOF_INT128_T, INT16, FLAGS )
#else
#define PARSEC_DATATYPE_INITIALIZER_INT16(FLAGS)      PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( INT16, FLAGS )
#endif
#define PARSEC_DATATYPE_INITIALIZER_UINT1(FLAGS)      PARSEC_DATATYPE_INIT_BASIC_DATATYPE( uint8_t, ALIGNOF_INT8_T, UINT1, FLAGS )
#define PARSEC_DATATYPE_INITIALIZER_UINT2(FLAGS)      PARSEC_DATATYPE_INIT_BASIC_DATATYPE( uint16_t, ALIGNOF_INT16_T, UINT2, FLAGS )
#define PARSEC_DATATYPE_INITIALIZER_UINT4(FLAGS)      PARSEC_DATATYPE_INIT_BASIC_DATATYPE( uint32_t, ALIGNOF_INT32_T, UINT4, FLAGS )
#define PARSEC_DATATYPE_INITIALIZER_UINT8(FLAGS)      PARSEC_DATATYPE_INIT_BASIC_DATATYPE( uint64_t, ALIGNOF_INT64_T, UINT8, FLAGS )
#ifdef HAVE_UINT128_T
#define PARSEC_DATATYPE_INITIALIZER_UINT16(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( uint128_t, ALIGNOF_INT128_T, UINT16, FLAGS )
#else
#define PARSEC_DATATYPE_INITIALIZER_UINT16(FLAGS)     PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( INT16, FLAGS )
#endif

#if defined(HAVE_SHORT_FLOAT) && SIZEOF_SHORT_FLOAT == 2
#define PARSEC_DATATYPE_INITIALIZER_FLOAT2(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( short float, ALIGNOF_SHORT_FLOAT, FLOAT2, FLAGS )
#elif SIZEOF_FLOAT == 2
#define PARSEC_DATATYPE_INITIALIZER_FLOAT2(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( float, ALIGNOF_FLOAT, FLOAT2, FLAGS )
#elif SIZEOF_DOUBLE == 2
#define PARSEC_DATATYPE_INITIALIZER_FLOAT2(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( double, ALIGNOF_DOUBLE, FLOAT2, FLAGS )
#elif SIZEOF_LONG_DOUBLE == 2
#define PARSEC_DATATYPE_INITIALIZER_FLOAT2(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( long double, ALIGNOF_LONG_DOUBLE, FLOAT2, FLAGS )
#elif defined(HAVE_PARSEC_SHORT_FLOAT_T) && SIZEOF_PARSEC_SHORT_FLOAT_T == 2
#define PARSEC_DATATYPE_INITIALIZER_FLOAT2(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( parsec_short_float_t, ALIGNOF_PARSEC_SHORT_FLOAT_T, FLOAT2, FLAGS )
#else
#define PARSEC_DATATYPE_INITIALIZER_FLOAT2(FLAGS)     PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( FLOAT2, FLAGS )
#endif

#if defined(HAVE_SHORT_FLOAT) && SIZEOF_SHORT_FLOAT == 4
#define PARSEC_DATATYPE_INITIALIZER_FLOAT4(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( short float, ALIGNOF_SHORT_FLOAT, FLOAT4, FLAGS )
#elif SIZEOF_FLOAT == 4
#define PARSEC_DATATYPE_INITIALIZER_FLOAT4(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( float, ALIGNOF_FLOAT, FLOAT4, FLAGS )
#elif SIZEOF_DOUBLE == 4
#define PARSEC_DATATYPE_INITIALIZER_FLOAT4(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( double, ALIGNOF_DOUBLE, FLOAT4, FLAGS )
#elif SIZEOF_LONG_DOUBLE == 4
#define PARSEC_DATATYPE_INITIALIZER_FLOAT4(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( long double, ALIGNOF_LONG_DOUBLE, FLOAT4, FLAGS )
#elif defined(HAVE_PARSEC_SHORT_FLOAT_T) && SIZEOF_PARSEC_SHORT_FLOAT_T == 4
#define PARSEC_DATATYPE_INITIALIZER_FLOAT4(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( parsec_short_float_t, ALIGNOF_PARSEC_SHORT_FLOAT_T, FLOAT4, FLAGS )
#else
#define PARSEC_DATATYPE_INITIALIZER_FLOAT4(FLAGS)     PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( FLOAT4, FLAGS )
#endif

#if defined(HAVE_SHORT_FLOAT) && SIZEOF_SHORT_FLOAT == 8
#define PARSEC_DATATYPE_INITIALIZER_FLOAT8(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( short float, ALIGNOF_SHORT_FLOAT, FLOAT8, FLAGS )
#elif SIZEOF_FLOAT == 8
#define PARSEC_DATATYPE_INITIALIZER_FLOAT8(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( float, ALIGNOF_FLOAT, FLOAT8, FLAGS )
#elif SIZEOF_DOUBLE == 8
#define PARSEC_DATATYPE_INITIALIZER_FLOAT8(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( double, ALIGNOF_DOUBLE, FLOAT8, FLAGS )
#elif SIZEOF_LONG_DOUBLE == 8
#define PARSEC_DATATYPE_INITIALIZER_FLOAT8(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( long double, ALIGNOF_LONG_DOUBLE, FLOAT8, FLAGS )
#elif defined(HAVE_PARSEC_SHORT_FLOAT_T) && SIZEOF_PARSEC_SHORT_FLOAT_T == 8
#define PARSEC_DATATYPE_INITIALIZER_FLOAT8(FLAGS)     PARSEC_DATATYPE_INIT_BASIC_DATATYPE( parsec_short_float_t, ALIGNOF_PARSEC_SHORT_FLOAT_T, FLOAT8, FLAGS )
#else
#define PARSEC_DATATYPE_INITIALIZER_FLOAT8(FLAGS)     PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( FLOAT8, FLAGS )
#endif

#if defined(HAVE_SHORT_FLOAT) && SIZEOF_SHORT_FLOAT == 12
#define PARSEC_DATATYPE_INITIALIZER_FLOAT12(FLAGS)    PARSEC_DATATYPE_INIT_BASIC_DATATYPE( short float, ALIGNOF_SHORT_FLOAT, FLOAT12, FLAGS )
#elif SIZEOF_FLOAT == 12
#define PARSEC_DATATYPE_INITIALIZER_FLOAT12(FLAGS)    PARSEC_DATATYPE_INIT_BASIC_DATATYPE( float, ALIGNOF_FLOAT, FLOAT12, FLAGS )
#elif SIZEOF_DOUBLE == 12
#define PARSEC_DATATYPE_INITIALIZER_FLOAT12(FLAGS)    PARSEC_DATATYPE_INIT_BASIC_DATATYPE( double, ALIGNOF_DOUBLE, FLOAT12, FLAGS )
#elif SIZEOF_LONG_DOUBLE == 12
#define PARSEC_DATATYPE_INITIALIZER_FLOAT12(FLAGS)    PARSEC_DATATYPE_INIT_BASIC_DATATYPE( long double, ALIGNOF_LONG_DOUBLE, FLOAT12, FLAGS )
#elif defined(HAVE_PARSEC_SHORT_FLOAT_T) && SIZEOF_PARSEC_SHORT_FLOAT_T == 12
#define PARSEC_DATATYPE_INITIALIZER_FLOAT12(FLAGS)    PARSEC_DATATYPE_INIT_BASIC_DATATYPE( parsec_short_float_t, ALIGNOF_PARSEC_SHORT_FLOAT_T, FLOAT12, FLAGS )
#else
#define PARSEC_DATATYPE_INITIALIZER_FLOAT12(FLAGS)    PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( FLOAT12, FLAGS )
#endif

#if defined(HAVE_SHORT_FLOAT) && SIZEOF_SHORT_FLOAT == 16
#define PARSEC_DATATYPE_INITIALIZER_FLOAT16(FLAGS)    PARSEC_DATATYPE_INIT_BASIC_DATATYPE( short float, ALIGNOF_SHORT_FLOAT, FLOAT16, FLAGS )
#elif SIZEOF_FLOAT == 16
#define PARSEC_DATATYPE_INITIALIZER_FLOAT16(FLAGS)    PARSEC_DATATYPE_INIT_BASIC_DATATYPE( float, ALIGNOF_FLOAT, FLOAT16, FLAGS )
#elif SIZEOF_DOUBLE == 16
#define PARSEC_DATATYPE_INITIALIZER_FLOAT16(FLAGS)    PARSEC_DATATYPE_INIT_BASIC_DATATYPE( double, ALIGNOF_DOUBLE, FLOAT16, FLAGS )
#elif SIZEOF_LONG_DOUBLE == 16
#define PARSEC_DATATYPE_INITIALIZER_FLOAT16(FLAGS)    PARSEC_DATATYPE_INIT_BASIC_DATATYPE( long double, ALIGNOF_LONG_DOUBLE, FLOAT16, FLAGS )
#elif defined(HAVE_PARSEC_SHORT_FLOAT_T) && SIZEOF_PARSEC_SHORT_FLOAT_T == 16
#define PARSEC_DATATYPE_INITIALIZER_FLOAT16(FLAGS)    PARSEC_DATATYPE_INIT_BASIC_DATATYPE( parsec_short_float_t, ALIGNOF_PARSEC_SHORT_FLOAT_T, FLOAT16, FLAGS )
#else
#define PARSEC_DATATYPE_INITIALIZER_FLOAT16(FLAGS)    PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( FLOAT16, FLAGS )
#endif

#if defined(HAVE_SHORT_FLOAT__COMPLEX)
#define PARSEC_DATATYPE_INITIALIZER_SHORT_FLOAT_COMPLEX(FLAGS) PARSEC_DATATYPE_INIT_BASIC_DATATYPE( short float _Complex, ALIGNOF_SHORT_FLOAT__COMPLEX, SHORT_FLOAT_COMPLEX, FLAGS )
#elif defined(HAVE_PARSEC_SHORT_FLOAT_COMPLEX_T)
#define PARSEC_DATATYPE_INITIALIZER_SHORT_FLOAT_COMPLEX(FLAGS) PARSEC_DATATYPE_INIT_BASIC_DATATYPE( parsec_short_float_complex_t, ALIGNOF_PARSEC_SHORT_FLOAT_COMPLEX_T, SHORT_FLOAT_COMPLEX, FLAGS )
#else
#define PARSEC_DATATYPE_INITIALIZER_SHORT_FLOAT_COMPLEX(FLAGS) PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( SHORT_FLOAT_COMPLEX, FLAGS)
#endif

#define PARSEC_DATATYPE_INITIALIZER_FLOAT_COMPLEX(FLAGS) PARSEC_DATATYPE_INIT_BASIC_DATATYPE( float _Complex, ALIGNOF_FLOAT__COMPLEX, FLOAT_COMPLEX, FLAGS )

#define PARSEC_DATATYPE_INITIALIZER_DOUBLE_COMPLEX(FLAGS) PARSEC_DATATYPE_INIT_BASIC_DATATYPE( double _Complex, ALIGNOF_DOUBLE__COMPLEX, DOUBLE_COMPLEX, FLAGS )

#define PARSEC_DATATYPE_INITIALIZER_LONG_DOUBLE_COMPLEX(FLAGS) PARSEC_DATATYPE_INIT_BASIC_DATATYPE( long double _Complex, ALIGNOF_LONG_DOUBLE__COMPLEX, LONG_DOUBLE_COMPLEX, FLAGS )

#define PARSEC_DATATYPE_INITIALIZER_BOOL(FLAGS)       PARSEC_DATATYPE_INIT_BASIC_DATATYPE( _Bool, ALIGNOF__BOOL, BOOL, FLAGS )

#if ALIGNOF_WCHAR_T != 0
#define PARSEC_DATATYPE_INITIALIZER_WCHAR(FLAGS)      PARSEC_DATATYPE_INIT_BASIC_DATATYPE( wchar_t, ALIGNOF_WCHAR_T, WCHAR, FLAGS )
#else
#define PARSEC_DATATYPE_INITIALIZER_WCHAR(FLAGS)      PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED( WCHAR, FLAGS )
#endif

#define BASIC_DDT_FROM_ELEM( ELEM ) (parsec_datatype_basicDatatypes[(ELEM).elem.common.type])

#define SAVE_STACK( PSTACK, INDEX, TYPE, COUNT, DISP) \
do { \
   (PSTACK)->index    = (INDEX); \
   (PSTACK)->type     = (TYPE); \
   (PSTACK)->count    = (COUNT); \
   (PSTACK)->disp     = (DISP); \
} while(0)

#define PUSH_STACK( PSTACK, STACK_POS, INDEX, TYPE, COUNT, DISP) \
do { \
   dt_stack_t* pTempStack = (PSTACK) + 1; \
   SAVE_STACK( pTempStack, (INDEX), (TYPE), (COUNT), (DISP) );  \
   (STACK_POS)++; \
   (PSTACK) = pTempStack; \
} while(0)

#if PARSEC_ENABLE_DEBUG
#define PARSEC_DATATYPE_SAFEGUARD_POINTER( ACTPTR, LENGTH, INITPTR, PDATA, COUNT ) \
    {                                                                   \
        unsigned char *__lower_bound = (INITPTR), *__upper_bound;       \
        assert( ((LENGTH) != 0) && ((COUNT) != 0) );                    \
        __lower_bound += (PDATA)->true_lb;                              \
        __upper_bound = (INITPTR) + (PDATA)->true_ub +                  \
            ((PDATA)->ub - (PDATA)->lb) * ((COUNT) - 1);                \
        if( ((ACTPTR) < __lower_bound) || ((ACTPTR) >= __upper_bound) ) { \
            parsec_datatype_safeguard_pointer_debug_breakpoint( (ACTPTR), (LENGTH), (INITPTR), (PDATA), (COUNT) ); \
            parsec_output( 0, "%s:%d\n\tPointer %p size %lu is outside [%p,%p] for\n\tbase ptr %p count %lu and data \n", \
                         __FILE__, __LINE__, (void*)(ACTPTR), (unsigned long)(LENGTH), (void*)__lower_bound, (void*)__upper_bound, \
                         (void*)(INITPTR), (unsigned long)(COUNT) );    \
            parsec_datatype_dump( (PDATA) );                              \
        }                                                               \
    }

#else
#define PARSEC_DATATYPE_SAFEGUARD_POINTER( ACTPTR, LENGTH, INITPTR, PDATA, COUNT )
#endif  /* PARSEC_ENABLE_DEBUG */

static inline int GET_FIRST_NON_LOOP( const union dt_elem_desc* _pElem )
{
    int element_index = 0;

    /* We dont have to check for the end as we always put an END_LOOP
     * at the end of all datatype descriptions.
     */
    while( _pElem->elem.common.type == PARSEC_DATATYPE_LOOP ) {
        ++_pElem; element_index++;
    }
    return element_index;
}

#define UPDATE_INTERNAL_COUNTERS( DESCRIPTION, POSITION, ELEMENT, COUNTER ) \
    do {                                                                    \
        (ELEMENT) = &((DESCRIPTION)[(POSITION)]);                           \
        if( PARSEC_DATATYPE_LOOP == (ELEMENT)->elem.common.type )             \
            (COUNTER) = (ELEMENT)->loop.loops;                              \
        else                                                                \
            (COUNTER) = (ELEMENT)->elem.count * (ELEMENT)->elem.blocklen;   \
    } while (0)

PARSEC_DECLSPEC int parsec_datatype_contain_basic_datatypes( const struct parsec_datatype_s* pData, char* ptr, size_t length );
PARSEC_DECLSPEC int parsec_datatype_dump_data_flags( unsigned short usflags, char* ptr, size_t length );
PARSEC_DECLSPEC int parsec_datatype_dump_data_desc( union dt_elem_desc* pDesc, int nbElems, char* ptr, size_t length );

extern bool parsec_ddt_position_debug;
extern bool parsec_ddt_copy_debug;
extern bool parsec_ddt_unpack_debug;
extern bool parsec_ddt_pack_debug;
extern bool parsec_ddt_raw_debug;

END_C_DECLS
#endif  /* _PARSEC_DATATYPE_INTERNAL_H_HAS_BEEN_INCLUDED */
