/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

int32_t parsec_datatype_resize( parsec_datatype_s* type, ptrdiff_t lb, ptrdiff_t extent )
{
    type->lb = lb;
    type->ub = lb + extent;

    type->flags &= ~PARSEC_DATATYPE_FLAG_NO_GAPS;
    if( (extent == (ptrdiff_t)type->size) &&
        (type->flags & PARSEC_DATATYPE_FLAG_CONTIGUOUS) ) {
        type->flags |= PARSEC_DATATYPE_FLAG_NO_GAPS;
    }
    return PARSEC_SUCCESS;
}
