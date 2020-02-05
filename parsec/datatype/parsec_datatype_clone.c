/* -*- Mode: C; c-basic-offset:4 ; -*- */
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
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2019      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "parsec/parsec_config.h"
#include "parsec/datatype/parsec_datatype_config.h"
#include "parsec/constants.h"
#include "parsec/datatype/parsec_datatype.h"
#include "parsec/datatype/parsec_datatype_internal.h"

/*
 * As the new type has the same commit state as the old one, I have to copy the fake
 * PARSEC_DATATYPE_END_LOOP from the description (both normal and optimized).
 *
 * Clone all the values from oldType into newType without allocating a new datatype.
 */
int32_t parsec_datatype_clone( const parsec_datatype_s * src_type, parsec_datatype_s * dest_type )
{
    int32_t desc_length = src_type->desc.used + 1;  /* +1 because of the fake PARSEC_DATATYPE_END_LOOP entry */
    dt_elem_desc_t* temp = dest_type->desc.desc;    /* temporary copy of the desc pointer */

    /* copy _excluding_ the super object, we want to keep the cls_destruct_array */
    memcpy( (char*)dest_type + sizeof(parsec_object_t),
            (char*)src_type + sizeof(parsec_object_t),
            sizeof(parsec_datatype_s)-sizeof(parsec_object_t) );

    dest_type->flags &= (~PARSEC_DATATYPE_FLAG_PREDEFINED);
    dest_type->ptypes = NULL;
    dest_type->desc.desc = temp;

    /**
     * Allow duplication of MPI_UB and MPI_LB.
     */
    if( 0 != src_type->desc.used ) {
        memcpy( dest_type->desc.desc, src_type->desc.desc, sizeof(dt_elem_desc_t) * desc_length );
        if( 0 != src_type->opt_desc.used ) {
            if( src_type->opt_desc.desc == src_type->desc.desc) {
                dest_type->opt_desc = dest_type->desc;
            } else {
                desc_length = dest_type->opt_desc.used + 1;
                dest_type->opt_desc.desc = (dt_elem_desc_t*)malloc( desc_length * sizeof(dt_elem_desc_t) );
                /*
                 * Yes, the dest_type->opt_desc.length is just the opt_desc.used of the old Type.
                 */
                dest_type->opt_desc.length = src_type->opt_desc.used;
                dest_type->opt_desc.used = src_type->opt_desc.used;
                memcpy( dest_type->opt_desc.desc, src_type->opt_desc.desc, desc_length * sizeof(dt_elem_desc_t) );
            }
        } else {
            assert( NULL == dest_type->opt_desc.desc );
            assert( 0 == dest_type->opt_desc.length );
        }
    }
    dest_type->id  = src_type->id;  /* preserve the default id. This allow us to
                                     * copy predefined types. */
    return PARSEC_SUCCESS;
}
