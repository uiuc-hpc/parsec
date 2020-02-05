/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2017-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      Triad National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * parsec_datatype_s interface for PARSEC internal data type representation
 *
 * parsec_datatype_s is a class which represents contiguous or
 * non-contiguous data together with constituent type-related
 * information.
 */

#ifndef _PARSEC_DATATYPE_H_HAS_BEEN_INCLUDED
#define _PARSEC_DATATYPE_H_HAS_BEEN_INCLUDED

#include "parsec/parsec_config.h"
#include "parsec/datatype/parsec_datatype_config.h"

#include <stddef.h>

#include "parsec/class/parsec_object.h"

BEGIN_C_DECLS

/*
 * If there are more basic datatypes than the number of bytes in the int type
 * the bdt_used field of the data description struct should be changed to long.
 *
 * This must match the same definition as in parsec_datatype_internal.h
 */
#if !defined(PARSEC_DATATYPE_MAX_PREDEFINED)
#define PARSEC_DATATYPE_MAX_PREDEFINED 26
#endif
/*
 * No more than this number of _Basic_ datatypes in C/CPP or Fortran
 * are supported (in order to not change setup and usage of the predefined
 * datatypes).
 *
 * BEWARE: This constant should reflect whatever the OMPI-layer needs.
 */
#define PARSEC_DATATYPE_MAX_SUPPORTED  50


/* flags for the datatypes. */
#define PARSEC_DATATYPE_FLAG_UNAVAILABLE   0x0001  /**< datatypes unavailable on the build (OS or compiler dependant) */
#define PARSEC_DATATYPE_FLAG_PREDEFINED    0x0002  /**< cannot be removed: initial and predefined datatypes */
#define PARSEC_DATATYPE_FLAG_COMMITTED     0x0004  /**< ready to be used for a send/recv operation */
#define PARSEC_DATATYPE_FLAG_OVERLAP       0x0008  /**< datatype is unpropper for a recv operation */
#define PARSEC_DATATYPE_FLAG_CONTIGUOUS    0x0010  /**< contiguous datatype */
#define PARSEC_DATATYPE_FLAG_NO_GAPS       0x0020  /**< no gaps around the datatype, aka PARSEC_DATATYPE_FLAG_CONTIGUOUS and extent == size */
#define PARSEC_DATATYPE_FLAG_USER_LB       0x0040  /**< has a user defined LB */
#define PARSEC_DATATYPE_FLAG_USER_UB       0x0080  /**< has a user defined UB */
#define PARSEC_DATATYPE_FLAG_DATA          0x0100  /**< data or control structure */
/*
 * We should make the difference here between the predefined contiguous and non contiguous
 * datatypes. The PARSEC_DATATYPE_FLAG_BASIC is held by all predefined contiguous datatypes.
 */
#define PARSEC_DATATYPE_FLAG_BASIC         (PARSEC_DATATYPE_FLAG_PREDEFINED | \
                                          PARSEC_DATATYPE_FLAG_CONTIGUOUS | \
                                          PARSEC_DATATYPE_FLAG_NO_GAPS |    \
                                          PARSEC_DATATYPE_FLAG_DATA |       \
                                          PARSEC_DATATYPE_FLAG_COMMITTED)

/**
 * The number of supported entries in the data-type definition and the
 * associated type.
 */
#define MAX_DT_COMPONENT_COUNT UINT_MAX
typedef size_t parsec_datatype_count_t;

typedef union dt_elem_desc dt_elem_desc_t;

struct dt_type_desc_t {
    parsec_datatype_count_t  length;  /**< the maximum number of elements in the description array */
    parsec_datatype_count_t  used;    /**< the number of used elements in the description array */
    dt_elem_desc_t*        desc;
};
typedef struct dt_type_desc_t dt_type_desc_t;


/*
 * The datatype description.
 */
struct parsec_datatype_s {
    parsec_object_t      super;    /**< basic superclass */
    uint16_t           flags;    /**< the flags */
    uint16_t           id;       /**< data id, normally the index in the data array. */
    uint32_t           bdt_used; /**< bitset of which basic datatypes are used in the data description */
    size_t             size;     /**< total size in bytes of the memory used by the data if
                                      the data is put on a contiguous buffer */
    ptrdiff_t          true_lb;  /**< the true lb of the data without user defined lb and ub */
    ptrdiff_t          true_ub;  /**< the true ub of the data without user defined lb and ub */
    ptrdiff_t          lb;       /**< lower bound in memory */
    ptrdiff_t          ub;       /**< upper bound in memory */
    /* --- cacheline 1 boundary (64 bytes) --- */
    size_t             nbElems;  /**< total number of elements inside the datatype */
    uint32_t           align;    /**< data should be aligned to */
    uint32_t           loops;    /**< number of loops on the iternal type stack */

    /* Attribute fields */
    char               name[PARSEC_MAX_OBJECT_NAME];  /**< name of the datatype */
    dt_type_desc_t     desc;     /**< the data description */
    dt_type_desc_t     opt_desc; /**< short description of the data used when conversion is useless
                                      or in the send case (without conversion) */

    size_t             *ptypes;  /**< array of basic predefined types that facilitate the computing
                                      of the remote size in heterogeneous environments. The length of the
                                      array is dependent on the maximum number of predefined datatypes of
                                      all language interfaces (because Fortran is not known at the PARSEC
                                      layer). This field should never be initialized in homogeneous
                                      environments */
    /* --- cacheline 5 boundary (320 bytes) was 32-36 bytes ago --- */

    /* size: 352, cachelines: 6, members: 15 */
    /* last cacheline: 28-32 bytes */
};

typedef struct parsec_datatype_s parsec_datatype_s;

PARSEC_DECLSPEC OBJ_CLASS_DECLARATION( parsec_datatype_s );

PARSEC_DECLSPEC extern const parsec_datatype_s* parsec_datatype_basicDatatypes[PARSEC_DATATYPE_MAX_PREDEFINED];
PARSEC_DECLSPEC extern const size_t parsec_datatype_local_sizes[PARSEC_DATATYPE_MAX_PREDEFINED];

/* Local Architecture as provided by parsec_arch_compute_local_id() */
PARSEC_DECLSPEC extern uint32_t parsec_local_arch;

/*
 * The PARSEC-layer's Basic datatypes themselves.
 */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_empty;
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_loop;
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_end_loop;
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_lb;
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_ub;
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_int1;       /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_int2;       /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_int4;       /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_int8;       /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_int16;      /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_uint1;      /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_uint2;      /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_uint4;      /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_uint8;      /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_uint16;     /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_float2;     /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_float4;     /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_float8;     /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_float12;    /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_float16;    /* in bytes */
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_short_float_complex;
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_float_complex;
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_double_complex;
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_long_double_complex;
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_bool;
PARSEC_DECLSPEC extern const parsec_datatype_s parsec_datatype_wchar;


/*
 * Functions exported externally
 */
int parsec_datatype_register_params(void);
PARSEC_DECLSPEC int32_t parsec_datatype_init( void );
PARSEC_DECLSPEC parsec_datatype_s* parsec_datatype_create( int32_t expectedSize );
PARSEC_DECLSPEC int32_t parsec_datatype_create_desc( parsec_datatype_s * datatype, int32_t expectedSize );
PARSEC_DECLSPEC int32_t parsec_datatype_commit( parsec_datatype_s * pData );
PARSEC_DECLSPEC int32_t parsec_datatype_destroy( parsec_datatype_s** );
PARSEC_DECLSPEC int32_t parsec_datatype_is_monotonic( parsec_datatype_s* type);

static inline int32_t
parsec_datatype_is_committed( const parsec_datatype_s* type )
{
    return ((type->flags & PARSEC_DATATYPE_FLAG_COMMITTED) == PARSEC_DATATYPE_FLAG_COMMITTED);
}

static inline int32_t
parsec_datatype_is_overlapped( const parsec_datatype_s* type )
{
    return ((type->flags & PARSEC_DATATYPE_FLAG_OVERLAP) == PARSEC_DATATYPE_FLAG_OVERLAP);
}

static inline int32_t
parsec_datatype_is_valid( const parsec_datatype_s* type )
{
    return !((type->flags & PARSEC_DATATYPE_FLAG_UNAVAILABLE) == PARSEC_DATATYPE_FLAG_UNAVAILABLE);
}

static inline int32_t
parsec_datatype_is_predefined( const parsec_datatype_s* type )
{
    return (type->flags & PARSEC_DATATYPE_FLAG_PREDEFINED);
}

/*
 * This function return true (1) if the datatype representation depending on the count
 * is contiguous in the memory. And false (0) otherwise.
 */
static inline int32_t
parsec_datatype_is_contiguous_memory_layout( const parsec_datatype_s* datatype, int32_t count )
{
    if( !(datatype->flags & PARSEC_DATATYPE_FLAG_CONTIGUOUS) ) return 0;
    if( (count == 1) || (datatype->flags & PARSEC_DATATYPE_FLAG_NO_GAPS) ) return 1;
    return 0;
}


PARSEC_DECLSPEC void
parsec_datatype_dump( const parsec_datatype_s* pData );

/* data creation functions */

/**
 * Create a duplicate of the source datatype.
 */
PARSEC_DECLSPEC int32_t
parsec_datatype_clone( const parsec_datatype_s* src_type,
                     parsec_datatype_s* dest_type );
/**
 * A contiguous array of identical datatypes.
 */
PARSEC_DECLSPEC int32_t
parsec_datatype_create_contiguous( int count, const parsec_datatype_s* oldType,
                                 parsec_datatype_s** newType );
/**
 * Add a new datatype to the base type description. The count is the number
 * repetitions of the same element to be added, and the extent is the extent
 * of each element. The displacement is the initial displacement of the
 * first element.
 */
PARSEC_DECLSPEC int32_t
parsec_datatype_add( parsec_datatype_s* pdtBase,
                   const parsec_datatype_s* pdtAdd, size_t count,
                   ptrdiff_t disp, ptrdiff_t extent );

/**
 * Alter the lb and extent of an existing datatype in place.
 */
PARSEC_DECLSPEC int32_t
parsec_datatype_resize( parsec_datatype_s* type,
                      ptrdiff_t lb,
                      ptrdiff_t extent );

static inline int32_t
parsec_datatype_type_lb( const parsec_datatype_s* pData, ptrdiff_t* disp )
{
    *disp = pData->lb;
    return 0;
}

static inline int32_t
parsec_datatype_type_ub( const parsec_datatype_s* pData, ptrdiff_t* disp )
{
    *disp = pData->ub;
    return 0;
}

static inline int32_t
parsec_datatype_type_size( const parsec_datatype_s* pData, size_t *size )
{
    *size = pData->size;
    return 0;
}

static inline int32_t
parsec_datatype_type_extent( const parsec_datatype_s* pData, ptrdiff_t* extent )
{
    *extent = pData->ub - pData->lb;
    return 0;
}

static inline int32_t
parsec_datatype_get_extent( const parsec_datatype_s* pData, ptrdiff_t* lb, ptrdiff_t* extent)
{
    *lb = pData->lb; *extent = pData->ub - pData->lb;
    return 0;
}

static inline int32_t
parsec_datatype_get_true_extent( const parsec_datatype_s* pData, ptrdiff_t* true_lb, ptrdiff_t* true_extent)
{
    *true_lb = pData->true_lb;
    *true_extent = (pData->true_ub - pData->true_lb);
    return 0;
}

PARSEC_DECLSPEC ssize_t
parsec_datatype_get_element_count( const parsec_datatype_s* pData, size_t iSize );
PARSEC_DECLSPEC int32_t
parsec_datatype_set_element_count( const parsec_datatype_s* pData, size_t count, size_t* length );
PARSEC_DECLSPEC int32_t
parsec_datatype_copy_content_same_ddt( const parsec_datatype_s* pData, int32_t count,
                                     char* pDestBuf, char* pSrcBuf );

PARSEC_DECLSPEC int parsec_datatype_compute_ptypes( parsec_datatype_s* datatype );

PARSEC_DECLSPEC const parsec_datatype_s*
parsec_datatype_match_size( int size, uint16_t datakind, uint16_t datalang );

/*
 *
 */
PARSEC_DECLSPEC int32_t
parsec_datatype_sndrcv( void *sbuf, int32_t scount, const parsec_datatype_s* sdtype, void *rbuf,
                      int32_t rcount, const parsec_datatype_s* rdtype);

/*
 *
 */
PARSEC_DECLSPEC int32_t
parsec_datatype_get_args( const parsec_datatype_s* pData, int32_t which,
                        int32_t * ci, int32_t * i,
                        int32_t * ca, ptrdiff_t* a,
                        int32_t * cd, parsec_datatype_s** d, int32_t * type);
PARSEC_DECLSPEC int32_t
parsec_datatype_set_args( parsec_datatype_s* pData,
                        int32_t ci, int32_t ** i,
                        int32_t ca, ptrdiff_t* a,
                        int32_t cd, parsec_datatype_s** d,int32_t type);
PARSEC_DECLSPEC int32_t
parsec_datatype_copy_args( const parsec_datatype_s* source_data,
                         parsec_datatype_s* dest_data );
PARSEC_DECLSPEC int32_t
parsec_datatype_release_args( parsec_datatype_s* pData );

/*
 *
 */
PARSEC_DECLSPEC size_t
parsec_datatype_pack_description_length( const parsec_datatype_s* datatype );

/*
 *
 */
PARSEC_DECLSPEC int
parsec_datatype_get_pack_description( parsec_datatype_s* datatype,
                                    const void** packed_buffer );

/*
 *
 */
struct parsec_proc_t;
PARSEC_DECLSPEC parsec_datatype_s*
parsec_datatype_create_from_packed_description( void** packed_buffer,
                                              struct parsec_proc_t* remote_processor );

/* Compute the span in memory of count datatypes. This function help with temporary
 * memory allocations for receiving already typed data (such as those used for reduce
 * operations). This span is the distance between the minimum and the maximum byte
 * in the memory layout of count datatypes, or in other terms the memory needed to
 * allocate count times the datatype without the gap in the beginning and at the end.
 *
 * Returns: the memory span of count repetition of the datatype, and in the gap
 *          argument, the number of bytes of the gap at the beginning.
 */
static inline ptrdiff_t
parsec_datatype_span( const parsec_datatype_s* pData, size_t count,
                    ptrdiff_t* gap)
{
    if (PARSEC_UNLIKELY(0 == pData->size) || (0 == count)) {
        *gap = 0;
        return 0;
    }
    *gap = pData->true_lb;
    ptrdiff_t extent = (pData->ub - pData->lb);
    ptrdiff_t true_extent = (pData->true_ub - pData->true_lb);
    return true_extent + extent * (count - 1);
}

#if PARSEC_ENABLE_DEBUG
/*
 * Set a breakpoint to this function in your favorite debugger
 * to make it stop on all pack and unpack errors.
 */
PARSEC_DECLSPEC int
parsec_datatype_safeguard_pointer_debug_breakpoint( const void* actual_ptr, int length,
                                                  const void* initial_ptr,
                                                  const parsec_datatype_s* pData,
                                                  int count );
#endif  /* PARSEC_ENABLE_DEBUG */

END_C_DECLS
#endif  /* _PARSEC_DATATYPE_H_HAS_BEEN_INCLUDED */
