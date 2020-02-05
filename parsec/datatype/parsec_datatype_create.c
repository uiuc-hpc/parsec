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

#include <stddef.h>

#include "parsec/constants.h"
#include "parsec/datatype/parsec_datatype.h"
#include "parsec/datatype/parsec_datatype_internal.h"
#include "limits.h"

static void parsec_datatype_construct( parsec_datatype_s* pData )
{
    pData->size               = 0;
    pData->flags              = PARSEC_DATATYPE_FLAG_CONTIGUOUS;
    pData->id                 = 0;
    pData->bdt_used           = 0;
    pData->size               = 0;
    pData->true_lb            = LONG_MAX;
    pData->true_ub            = LONG_MIN;
    pData->lb                 = LONG_MAX;
    pData->ub                 = LONG_MIN;
    pData->align              = 1;
    pData->nbElems            = 0;
    memset(pData->name, 0, PARSEC_MAX_DATATYPE_NAME);

    pData->desc.desc          = NULL;
    pData->desc.length        = 0;
    pData->desc.used          = 0;

    pData->opt_desc.desc      = NULL;
    pData->opt_desc.length    = 0;
    pData->opt_desc.used      = 0;

    pData->ptypes             = NULL;
    pData->loops              = 0;
}

static void parsec_datatype_destruct( parsec_datatype_s* datatype )
{
    /**
     * As the default description and the optimized description might point to the
     * same data description we should start by cleaning the optimized description.
     */
    if( NULL != datatype->opt_desc.desc ) {
        if( datatype->opt_desc.desc != datatype->desc.desc )
            free( datatype->opt_desc.desc );
        datatype->opt_desc.length = 0;
        datatype->opt_desc.used   = 0;
        datatype->opt_desc.desc   = NULL;
    }
    if (!parsec_datatype_is_predefined(datatype)) {
        if( NULL != datatype->desc.desc ) {
            free( datatype->desc.desc );
            datatype->desc.length = 0;
            datatype->desc.used   = 0;
            datatype->desc.desc   = NULL;
        }
    }
    /* dont free the ptypes of predefined types (it was not dynamically allocated) */
    if( (NULL != datatype->ptypes) && (!parsec_datatype_is_predefined(datatype)) ) {
        free(datatype->ptypes);
        datatype->ptypes = NULL;
    }

    /* make sure the name is set to empty */
    datatype->name[0] = '\0';
}

PARSEC_OBJ_CLASS_INSTANCE(parsec_datatype_s, parsec_object_t, parsec_datatype_construct, parsec_datatype_destruct);

parsec_datatype_s* parsec_datatype_create( int32_t expectedSize )
{
    parsec_datatype_s* datatype = (parsec_datatype_s*)PARSEC_OBJ_NEW(parsec_datatype_s);

    if( expectedSize == -1 ) expectedSize = DT_INCREASE_STACK;
    datatype->desc.length = expectedSize + 1;  /* one for the fake elem at the end */
    datatype->desc.used   = 0;
    datatype->desc.desc   = (dt_elem_desc_t*)calloc(datatype->desc.length, sizeof(dt_elem_desc_t));
    /* BEWARE: an upper-layer configured with PARSEC_MAX_DATATYPE_NAME different than the PARSEC-layer will not work! */
    memset( datatype->name, 0, PARSEC_MAX_DATATYPE_NAME );
    return datatype;
}

int32_t parsec_datatype_create_desc( parsec_datatype_s * datatype, int32_t expectedSize )
{
    if( expectedSize == -1 )
        expectedSize = DT_INCREASE_STACK;
    datatype->desc.length = expectedSize + 1;  /* one for the fake elem at the end */
    datatype->desc.used   = 0;
    datatype->desc.desc   = (dt_elem_desc_t*)calloc(datatype->desc.length, sizeof(dt_elem_desc_t));
    if (NULL == datatype->desc.desc)
        return PARSEC_ERR_OUT_OF_RESOURCE;
    return PARSEC_SUCCESS;
}
