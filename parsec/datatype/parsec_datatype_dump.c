/* -*- Mode: C; c-basic-offset:4 ; -*- */
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
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2018      Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "parsec/parsec_config.h"
#include "parsec/datatype/parsec_datatype_config.h"

#include <stdlib.h>
#include <stdio.h>

#include "parsec/constants.h"
#include "parsec/utils/output.h"
#include "parsec/datatype/parsec_datatype.h"
#include "parsec/datatype/parsec_datatype_internal.h"

/********************************************************
 * Data dumping functions
 ********************************************************/

int parsec_datatype_contain_basic_datatypes( const parsec_datatype_s* pData, char* ptr, size_t length )
{
    int i;
    int32_t index = 0;
    uint64_t mask = 1;

    if( pData->flags & PARSEC_DATATYPE_FLAG_USER_LB ) index += snprintf( ptr, length - index, "lb " );
    if( pData->flags & PARSEC_DATATYPE_FLAG_USER_UB ) index += snprintf( ptr + index, length - index, "ub " );
    for( i = 0; i < PARSEC_DATATYPE_MAX_PREDEFINED; i++ ) {
        if( pData->bdt_used & mask ) {
            if( NULL == pData->ptypes ) {
                index += snprintf( ptr + index, length - index, "%s:* ", parsec_datatype_basicDatatypes[i]->name );
            } else {
                index += snprintf( ptr + index, length - index, "%s:%" PRIsize_t " ", parsec_datatype_basicDatatypes[i]->name,
                                   pData->ptypes[i]);
            }
        }
        mask <<= 1;
        if( length <= (size_t)index ) break;
    }
    return index;
}

int parsec_datatype_dump_data_flags( unsigned short usflags, char* ptr, size_t length )
{
    int index = 0;
    if( length < 22 ) return 0;
    index = snprintf( ptr, 22, "-----------[---][---]" );  /* set everything to - */
    if( usflags & PARSEC_DATATYPE_FLAG_COMMITTED )  ptr[1]  = 'c';
    if( usflags & PARSEC_DATATYPE_FLAG_CONTIGUOUS ) ptr[2]  = 'C';
    if( usflags & PARSEC_DATATYPE_FLAG_OVERLAP )    ptr[3]  = 'o';
    if( usflags & PARSEC_DATATYPE_FLAG_USER_LB )    ptr[4]  = 'l';
    if( usflags & PARSEC_DATATYPE_FLAG_USER_UB )    ptr[5]  = 'u';
    if( usflags & PARSEC_DATATYPE_FLAG_PREDEFINED ) ptr[6]  = 'P';
    if( !(usflags & PARSEC_DATATYPE_FLAG_NO_GAPS) ) ptr[7]  = 'G';
    if( usflags & PARSEC_DATATYPE_FLAG_DATA )       ptr[8]  = 'D';
    if( (usflags & PARSEC_DATATYPE_FLAG_BASIC) == PARSEC_DATATYPE_FLAG_BASIC ) ptr[9]  = 'B';
    /* We know nothing about the upper level language or flags! */
    /* ... */
    return index;
}


int parsec_datatype_dump_data_desc( dt_elem_desc_t* pDesc, int nbElems, char* ptr, size_t length )
{
    int i;
    int32_t index = 0;

    for( i = 0; i < nbElems; i++ ) {
        index += parsec_datatype_dump_data_flags( pDesc->elem.common.flags, ptr + index, length );
        if( length <= (size_t)index ) break;
        index += snprintf( ptr + index, length - index, "%15s ", parsec_datatype_basicDatatypes[pDesc->elem.common.type]->name );
        if( length <= (size_t)index ) break;
        if( PARSEC_DATATYPE_LOOP == pDesc->elem.common.type )
            index += snprintf( ptr + index, length - index, "%u times the next %u elements extent %td\n",
                               pDesc->loop.loops, pDesc->loop.items,
                               pDesc->loop.extent );
        else if( PARSEC_DATATYPE_END_LOOP == pDesc->elem.common.type )
            index += snprintf( ptr + index, length - index, "prev %u elements first elem displacement %td size of data %" PRIsize_t "\n",
                               pDesc->end_loop.items, pDesc->end_loop.first_elem_disp,
                               pDesc->end_loop.size );
        else
            index += snprintf( ptr + index, length - index, "count %u disp 0x%tx (%td) blen %" PRIsize_t " extent %td (size %zd)\n",
                               pDesc->elem.count, pDesc->elem.disp, pDesc->elem.disp, pDesc->elem.blocklen,
                               pDesc->elem.extent, (pDesc->elem.count * pDesc->elem.blocklen * parsec_datatype_basicDatatypes[pDesc->elem.common.type]->size) );
        pDesc++;

        if( length <= (size_t)index ) break;
    }
    return index;
}


void parsec_datatype_dump( const parsec_datatype_s* pData )
{
    size_t length;
    int index = 0;
    char* buffer;

    length = pData->opt_desc.used + pData->desc.used;
    length = length * 100 + 500;
    buffer = (char*)malloc( length );
    index += snprintf( buffer, length - index, "Datatype %p[%s] size %" PRIsize_t " align %u id %u length %" PRIsize_t " used %" PRIsize_t "\n"
                                               "true_lb %td true_ub %td (true_extent %td) lb %td ub %td (extent %td)\n"
                                               "nbElems %" PRIsize_t " loops %u flags %X (",
                       (void*)pData, pData->name, pData->size, pData->align, (uint32_t)pData->id, pData->desc.length, pData->desc.used,
                       pData->true_lb, pData->true_ub, pData->true_ub - pData->true_lb,
                       pData->lb, pData->ub, pData->ub - pData->lb,
                       pData->nbElems, pData->loops, (int)pData->flags );
    /* dump the flags */
    if( pData->flags == PARSEC_DATATYPE_FLAG_PREDEFINED )
        index += snprintf( buffer + index, length - index, "predefined " );
    else {
        if( pData->flags & PARSEC_DATATYPE_FLAG_COMMITTED ) index += snprintf( buffer + index, length - index, "committed " );
        if( pData->flags & PARSEC_DATATYPE_FLAG_CONTIGUOUS) index += snprintf( buffer + index, length - index, "contiguous " );
    }
    index += snprintf( buffer + index, length - index, ")" );
    index += parsec_datatype_dump_data_flags( pData->flags, buffer + index, length - index );
    {
        index += snprintf( buffer + index, length - index, "\n   contain " );
        index += parsec_datatype_contain_basic_datatypes( pData, buffer + index, length - index );
        index += snprintf( buffer + index, length - index, "\n" );
    }
    if( (pData->opt_desc.desc != pData->desc.desc) && (NULL != pData->opt_desc.desc) ) {
        /* If the data is already committed print everything including the last
         * fake PARSEC_DATATYPE_END_LOOP entry.
         */
        index += parsec_datatype_dump_data_desc( pData->desc.desc, pData->desc.used + 1, buffer + index, length - index );
        index += snprintf( buffer + index, length - index, "Optimized description \n" );
        index += parsec_datatype_dump_data_desc( pData->opt_desc.desc, pData->opt_desc.used + 1, buffer + index, length - index );
    } else {
        index += parsec_datatype_dump_data_desc( pData->desc.desc, pData->desc.used, buffer + index, length - index );
        index += snprintf( buffer + index, length - index, "No optimized description\n" );
    }
    buffer[index] = '\0';  /* make sure we end the string with 0 */
    parsec_output( 0, "%s\n", buffer );

    free(buffer);
}
