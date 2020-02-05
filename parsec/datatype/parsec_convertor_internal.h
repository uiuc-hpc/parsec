/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef _PARSEC_CONVERTOR_INTERNAL_HAS_BEEN_INCLUDED
#define _PARSEC_CONVERTOR_INTERNAL_HAS_BEEN_INCLUDED

#include "parsec/parsec_config.h"
#include "parsec/datatype/parsec_datatype_config.h"

#include "parsec/datatype/parsec_convertor.h"

BEGIN_C_DECLS

typedef int32_t (*conversion_fct_t)( parsec_convertor_t* pConvertor, uint32_t count,
                                     const void* from, size_t from_len, ptrdiff_t from_extent,
                                     void* to, size_t to_length, ptrdiff_t to_extent,
                                     ptrdiff_t *advance );

typedef struct parsec_convertor_master_t {
    struct parsec_convertor_master_t* next;
    uint32_t                        remote_arch;
    uint32_t                        flags;
    uint32_t                        hetero_mask;
    const size_t                    remote_sizes[PARSEC_DATATYPE_MAX_PREDEFINED];
    conversion_fct_t*               pFunctions;   /**< the convertor functions pointer */
} parsec_convertor_master_t;

/*
 * Find or create a new master convertor based on a specific architecture. The master
 * convertor hold all informations related to a defined architecture, such as the sizes
 * of the predefined data-types, the conversion functions, ...
 */
parsec_convertor_master_t* parsec_convertor_find_or_create_master( uint32_t remote_arch );

/*
 * Destroy all pending master convertors. This function is usually called when we
 * shutdown the data-type engine, once all convertors have been destroyed.
 */
void parsec_convertor_destroy_masters( void );


END_C_DECLS

#endif  /* _PARSEC_CONVERTOR_INTERNAL_HAS_BEEN_INCLUDED */
