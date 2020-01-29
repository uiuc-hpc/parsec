/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018-2019 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "parsec_config.h"

#include <stddef.h>

#include "parsec/constants.h"
#include "parsec/datatype/parsec_datatype.h"
#include "parsec/datatype/parsec_datatype_internal.h"
#include "parsec/datatype/parsec_convertor.h"

#define PARSEC_DATATYPE_MAX_MONOTONIC_IOVEC 32

/**
 * Check if the datatype describes a memory layout where the pointers to
 * the contiguous pieces are always advancing in the same direction, i.e.
 * there is no potential for overlap.
 */
int32_t parsec_datatype_is_monotonic(parsec_datatype_t* type )
{
    struct iovec iov[PARSEC_DATATYPE_MAX_MONOTONIC_IOVEC];
    ptrdiff_t upper_limit = (ptrdiff_t)type->true_lb;  /* as conversion base will be NULL the first address is true_lb */
    size_t max_data = 0x7FFFFFFF;
    parsec_convertor_t *pConv;
    bool monotonic = true;
    uint32_t iov_count;
    int rc;

    pConv  = parsec_convertor_create( parsec_local_arch, 0 );
    if (PARSEC_UNLIKELY(NULL == pConv)) {
        return -1;
    }
    rc = parsec_convertor_prepare_for_send( pConv, type, 1, NULL );
    if( PARSEC_UNLIKELY(PARSEC_SUCCESS != rc)) {
        OBJ_RELEASE(pConv);
        return -1;
    }

    do {
        iov_count = PARSEC_DATATYPE_MAX_MONOTONIC_IOVEC;
        rc = parsec_convertor_raw( pConv, iov, &iov_count, &max_data);
        for (uint32_t i = 0; i < iov_count; i++) {
            if ((ptrdiff_t)iov[i].iov_base < upper_limit) {
                monotonic = false;
                goto cleanup;
            }
            /* The new upper bound is at the end of the iovec */
            upper_limit = (ptrdiff_t)iov[i].iov_base + iov[i].iov_len;
        }
    } while (rc != 1);

  cleanup:
    OBJ_RELEASE( pConv );

    return monotonic;
}
