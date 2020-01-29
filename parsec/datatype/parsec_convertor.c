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
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2013-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "parsec_config.h"

#include <stddef.h>
#include <stdio.h>
#include <stdint.h>

#include "parsec/prefetch.h"
#include "parsec/util/arch.h"
#include "parsec/util/output.h"

#include "parsec/datatype/parsec_datatype_internal.h"
#include "parsec/datatype/parsec_datatype.h"
#include "parsec/datatype/parsec_convertor.h"
#include "parsec/datatype/parsec_datatype_checksum.h"
#include "parsec/datatype/parsec_datatype_prototypes.h"
#include "parsec/datatype/parsec_convertor_internal.h"
#if PARSEC_CUDA_SUPPORT
#include "parsec/datatype/parsec_datatype_cuda.h"
#define MEMCPY_CUDA( DST, SRC, BLENGTH, CONVERTOR ) \
    CONVERTOR->cbmemcpy( (DST), (SRC), (BLENGTH), (CONVERTOR) )
#endif

static void parsec_convertor_construct( parsec_convertor_t* convertor )
{
    convertor->pStack         = convertor->static_stack;
    convertor->stack_size     = DT_STATIC_STACK_SIZE;
    convertor->partial_length = 0;
    convertor->remoteArch     = parsec_local_arch;
    convertor->flags          = PARSEC_DATATYPE_FLAG_NO_GAPS | CONVERTOR_COMPLETED;
#if PARSEC_CUDA_SUPPORT
    convertor->cbmemcpy       = &parsec_cuda_memcpy;
#endif
}


static void parsec_convertor_destruct( parsec_convertor_t* convertor )
{
    parsec_convertor_cleanup( convertor );
}

OBJ_CLASS_INSTANCE(parsec_convertor_t, parsec_object_t, parsec_convertor_construct, parsec_convertor_destruct );

static parsec_convertor_master_t* parsec_convertor_master_list = NULL;

extern conversion_fct_t parsec_datatype_heterogeneous_copy_functions[PARSEC_DATATYPE_MAX_PREDEFINED];
extern conversion_fct_t parsec_datatype_copy_functions[PARSEC_DATATYPE_MAX_PREDEFINED];

void parsec_convertor_destroy_masters( void )
{
    parsec_convertor_master_t* master = parsec_convertor_master_list;

    while( NULL != master ) {
        parsec_convertor_master_list = master->next;
        master->next = NULL;
        /* Cleanup the conversion function if not one of the defaults */
        if( (master->pFunctions != parsec_datatype_heterogeneous_copy_functions) &&
            (master->pFunctions != parsec_datatype_copy_functions) )
            free( master->pFunctions );

        free( master );
        master = parsec_convertor_master_list;
    }
}

/**
 * Find or create a convertor suitable for the remote architecture. If there
 * is already a master convertor for this architecture then return it.
 * Otherwise, create and initialize a full featured master convertor.
 */
parsec_convertor_master_t* parsec_convertor_find_or_create_master( uint32_t remote_arch )
{
    parsec_convertor_master_t* master = parsec_convertor_master_list;
    int i;
    size_t* remote_sizes;

    while( NULL != master ) {
        if( master->remote_arch == remote_arch )
            return master;
        master = master->next;
    }
    /**
     * Create a new convertor matching the specified architecture and add it to the
     * master convertor list.
     */
    master = (parsec_convertor_master_t*)malloc( sizeof(parsec_convertor_master_t) );
    master->next = parsec_convertor_master_list;
    parsec_convertor_master_list = master;
    master->remote_arch = remote_arch;
    master->flags       = 0;
    master->hetero_mask = 0;
    /**
     * Most of the sizes will be identical, so for now just make a copy of
     * the local ones. As master->remote_sizes is defined as being an array of
     * consts we have to manually cast it before using it for writing purposes.
     */
    remote_sizes = (size_t*)master->remote_sizes;
    memcpy(remote_sizes, parsec_datatype_local_sizes, sizeof(size_t) * PARSEC_DATATYPE_MAX_PREDEFINED);
    /**
     * If the local and remote architecture are the same there is no need
     * to check for the remote data sizes. They will always be the same as
     * the local ones.
     */
    if( master->remote_arch == parsec_local_arch ) {
        master->pFunctions = parsec_datatype_copy_functions;
        master->flags |= CONVERTOR_HOMOGENEOUS;
        return master;
    }

    /* Find out the remote bool size */
    if( parsec_arch_checkmask( &master->remote_arch, PARSEC_ARCH_BOOLIS8 ) ) {
        remote_sizes[PARSEC_DATATYPE_BOOL] = 1;
    } else if( parsec_arch_checkmask( &master->remote_arch, PARSEC_ARCH_BOOLIS16 ) ) {
        remote_sizes[PARSEC_DATATYPE_BOOL] = 2;
    } else if( parsec_arch_checkmask( &master->remote_arch, PARSEC_ARCH_BOOLIS32 ) ) {
        remote_sizes[PARSEC_DATATYPE_BOOL] = 4;
    } else {
        parsec_output( 0, "Unknown sizeof(bool) for the remote architecture\n" );
    }

    /**
     * Now we can compute the conversion mask. For all sizes where the remote
     * and local architecture differ a conversion is needed. Moreover, if the
     * 2 architectures don't have the same endianess all data with a length
     * over 2 bytes (with the exception of logicals) have to be byte-swapped.
     */
    for( i = PARSEC_DATATYPE_FIRST_TYPE; i < PARSEC_DATATYPE_MAX_PREDEFINED; i++ ) {
        if( remote_sizes[i] != parsec_datatype_local_sizes[i] )
            master->hetero_mask |= (((uint32_t)1) << i);
    }
    if( parsec_arch_checkmask( &master->remote_arch, PARSEC_ARCH_ISBIGENDIAN ) !=
        parsec_arch_checkmask( &parsec_local_arch, PARSEC_ARCH_ISBIGENDIAN ) ) {
        uint32_t hetero_mask = 0;

        for( i = PARSEC_DATATYPE_FIRST_TYPE; i < PARSEC_DATATYPE_MAX_PREDEFINED; i++ ) {
            if( remote_sizes[i] > 1 )
                hetero_mask |= (((uint32_t)1) << i);
        }
        hetero_mask &= ~(((uint32_t)1) << PARSEC_DATATYPE_BOOL);
        master->hetero_mask |= hetero_mask;
    }
    master->pFunctions = (conversion_fct_t*)malloc( sizeof(parsec_datatype_heterogeneous_copy_functions) );
    /**
     * Usually the heterogeneous functions are slower than the copy ones. Let's
     * try to minimize the usage of the heterogeneous versions.
     */
    for( i = PARSEC_DATATYPE_FIRST_TYPE; i < PARSEC_DATATYPE_MAX_PREDEFINED; i++ ) {
        if( master->hetero_mask & (((uint32_t)1) << i) )
            master->pFunctions[i] = parsec_datatype_heterogeneous_copy_functions[i];
        else
            master->pFunctions[i] = parsec_datatype_copy_functions[i];
    }

    /* We're done so far, return the mater convertor */
    return master;
}


parsec_convertor_t* parsec_convertor_create( int32_t remote_arch, int32_t mode )
{
    parsec_convertor_t* convertor = OBJ_NEW(parsec_convertor_t);
    parsec_convertor_master_t* master;

    master = parsec_convertor_find_or_create_master( remote_arch );

    convertor->remoteArch = remote_arch;
    convertor->stack_pos  = 0;
    convertor->flags      = master->flags;
    convertor->master     = master;

    return convertor;
}

#define PARSEC_CONVERTOR_SET_STATUS_BEFORE_PACK_UNPACK( CONVERTOR, IOV, OUT, MAX_DATA ) \
    do {                                                                \
        /* protect against over packing data */                         \
        if( PARSEC_UNLIKELY((CONVERTOR)->flags & CONVERTOR_COMPLETED) ) { \
            (IOV)[0].iov_len = 0;                                       \
            *(OUT) = 0;                                                 \
            *(MAX_DATA) = 0;                                            \
            return 1;  /* nothing to do */                              \
        }                                                               \
        (CONVERTOR)->checksum = PARSEC_CSUM_ZERO;                         \
        (CONVERTOR)->csum_ui1 = 0;                                      \
        (CONVERTOR)->csum_ui2 = 0;                                      \
        assert( (CONVERTOR)->bConverted < (CONVERTOR)->local_size );    \
    } while(0)

/**
 * Return 0 if everything went OK and if there is still room before the complete
 *          conversion of the data (need additional call with others input buffers )
 *        1 if everything went fine and the data was completly converted
 *       -1 something wrong occurs.
 */
int32_t parsec_convertor_pack( parsec_convertor_t* pConv,
                             struct iovec* iov, uint32_t* out_size,
                             size_t* max_data )
{
    PARSEC_CONVERTOR_SET_STATUS_BEFORE_PACK_UNPACK( pConv, iov, out_size, max_data );

    if( PARSEC_LIKELY(pConv->flags & CONVERTOR_NO_OP) ) {
        /**
         * We are doing conversion on a contiguous datatype on a homogeneous
         * environment. The convertor contain minimal information, we only
         * use the bConverted to manage the conversion.
         */
        uint32_t i;
        unsigned char* base_pointer;
        size_t pending_length = pConv->local_size - pConv->bConverted;

        *max_data = pending_length;
        parsec_convertor_get_current_pointer( pConv, (void**)&base_pointer );

        for( i = 0; i < *out_size; i++ ) {
            if( iov[i].iov_len >= pending_length ) {
                goto complete_contiguous_data_pack;
            }
            if( PARSEC_LIKELY(NULL == iov[i].iov_base) )
                iov[i].iov_base = (IOVBASE_TYPE *) base_pointer;
            else
#if PARSEC_CUDA_SUPPORT
                MEMCPY_CUDA( iov[i].iov_base, base_pointer, iov[i].iov_len, pConv );
#else
                MEMCPY( iov[i].iov_base, base_pointer, iov[i].iov_len );
#endif
            pending_length -= iov[i].iov_len;
            base_pointer += iov[i].iov_len;
        }
        *max_data -= pending_length;
        pConv->bConverted += (*max_data);
        return 0;

complete_contiguous_data_pack:
        iov[i].iov_len = pending_length;
        if( PARSEC_LIKELY(NULL == iov[i].iov_base) )
            iov[i].iov_base = (IOVBASE_TYPE *) base_pointer;
        else
#if PARSEC_CUDA_SUPPORT
            MEMCPY_CUDA( iov[i].iov_base, base_pointer, iov[i].iov_len, pConv );
#else
            MEMCPY( iov[i].iov_base, base_pointer, iov[i].iov_len );
#endif
        pConv->bConverted = pConv->local_size;
        *out_size = i + 1;
        pConv->flags |= CONVERTOR_COMPLETED;
        return 1;
    }

    return pConv->fAdvance( pConv, iov, out_size, max_data );
}


int32_t parsec_convertor_unpack( parsec_convertor_t* pConv,
                               struct iovec* iov, uint32_t* out_size,
                               size_t* max_data )
{
    PARSEC_CONVERTOR_SET_STATUS_BEFORE_PACK_UNPACK( pConv, iov, out_size, max_data );

    if( PARSEC_LIKELY(pConv->flags & CONVERTOR_NO_OP) ) {
        /**
         * We are doing conversion on a contiguous datatype on a homogeneous
         * environment. The convertor contain minimal informations, we only
         * use the bConverted to manage the conversion.
         */
        uint32_t i;
        unsigned char* base_pointer;
        size_t pending_length = pConv->local_size - pConv->bConverted;

        *max_data = pending_length;
        parsec_convertor_get_current_pointer( pConv, (void**)&base_pointer );

        for( i = 0; i < *out_size; i++ ) {
            if( iov[i].iov_len >= pending_length ) {
                goto complete_contiguous_data_unpack;
            }
#if PARSEC_CUDA_SUPPORT
            MEMCPY_CUDA( base_pointer, iov[i].iov_base, iov[i].iov_len, pConv );
#else
            MEMCPY( base_pointer, iov[i].iov_base, iov[i].iov_len );
#endif
            pending_length -= iov[i].iov_len;
            base_pointer += iov[i].iov_len;
        }
        *max_data -= pending_length;
        pConv->bConverted += (*max_data);
        return 0;

complete_contiguous_data_unpack:
        iov[i].iov_len = pending_length;
#if PARSEC_CUDA_SUPPORT
        MEMCPY_CUDA( base_pointer, iov[i].iov_base, iov[i].iov_len, pConv );
#else
        MEMCPY( base_pointer, iov[i].iov_base, iov[i].iov_len );
#endif
        pConv->bConverted = pConv->local_size;
        *out_size = i + 1;
        pConv->flags |= CONVERTOR_COMPLETED;
        return 1;
    }

    return pConv->fAdvance( pConv, iov, out_size, max_data );
}

static inline int
parsec_convertor_create_stack_with_pos_contig( parsec_convertor_t* pConvertor,
                                             size_t starting_point, const size_t* sizes )
{
    dt_stack_t* pStack;   /* pointer to the position on the stack */
    const parsec_datatype_t* pData = pConvertor->pDesc;
    dt_elem_desc_t* pElems;
    size_t count;
    ptrdiff_t extent;

    pStack = pConvertor->pStack;
    /**
     * The prepare function already make the selection on which data representation
     * we have to use: normal one or the optimized version ?
     */
    pElems = pConvertor->use_desc->desc;

    count = starting_point / pData->size;
    extent = pData->ub - pData->lb;

    pStack[0].type     = PARSEC_DATATYPE_LOOP;  /* the first one is always the loop */
    pStack[0].count    = pConvertor->count - count;
    pStack[0].index    = -1;
    pStack[0].disp     = count * extent;

    /* now compute the number of pending bytes */
    count = starting_point % pData->size;
    /**
     * We save the current displacement starting from the begining
     * of this data.
     */
    if( PARSEC_LIKELY(0 == count) ) {
        pStack[1].type     = pElems->elem.common.type;
        pStack[1].count    = pElems->elem.blocklen;
    } else {
        pStack[1].type  = PARSEC_DATATYPE_UINT1;
        pStack[1].count = pData->size - count;
    }
    pStack[1].disp  = count;
    pStack[1].index = 0;  /* useless */

    pConvertor->bConverted = starting_point;
    pConvertor->stack_pos = 1;
    assert( 0 == pConvertor->partial_length );
    return PARSEC_SUCCESS;
}

static inline int
parsec_convertor_create_stack_at_begining( parsec_convertor_t* convertor,
                                         const size_t* sizes )
{
    dt_stack_t* pStack = convertor->pStack;
    dt_elem_desc_t* pElems;

    /**
     * The prepare function already make the selection on which data representation
     * we have to use: normal one or the optimized version ?
     */
    pElems = convertor->use_desc->desc;

    convertor->stack_pos      = 1;
    convertor->partial_length = 0;
    convertor->bConverted     = 0;
    /**
     * Fill the first position on the stack. This one correspond to the
     * last fake PARSEC_DATATYPE_END_LOOP that we add to the data representation and
     * allow us to move quickly inside the datatype when we have a count.
     */
    pStack[0].index = -1;
    pStack[0].count = convertor->count;
    pStack[0].disp  = 0;
    pStack[0].type  = PARSEC_DATATYPE_LOOP;

    pStack[1].index = 0;
    pStack[1].disp = 0;
    if( pElems[0].elem.common.type == PARSEC_DATATYPE_LOOP ) {
        pStack[1].count = pElems[0].loop.loops;
        pStack[1].type  = PARSEC_DATATYPE_LOOP;
    } else {
        pStack[1].count = (size_t)pElems[0].elem.count * pElems[0].elem.blocklen;
        pStack[1].type  = pElems[0].elem.common.type;
    }
    return PARSEC_SUCCESS;
}


int32_t parsec_convertor_set_position_nocheck( parsec_convertor_t* convertor,
                                             size_t* position )
{
    int32_t rc;

    /**
     * create_stack_with_pos_contig always set the position relative to the ZERO
     * position, so there is no need for special handling. In all other cases,
     * if we plan to rollback the convertor then first we have to reset it at
     * the beginning.
     */
    if( PARSEC_LIKELY(convertor->flags & PARSEC_DATATYPE_FLAG_CONTIGUOUS) ) {
        rc = parsec_convertor_create_stack_with_pos_contig( convertor, (*position),
                                                          parsec_datatype_local_sizes );
    } else {
        if( (0 == (*position)) || ((*position) < convertor->bConverted) ) {
            rc = parsec_convertor_create_stack_at_begining( convertor, parsec_datatype_local_sizes );
            if( 0 == (*position) ) return rc;
        }
        rc = parsec_convertor_generic_simple_position( convertor, position );
        /**
         * If we have a non-contigous send convertor don't allow it move in the middle
         * of a predefined datatype, it won't be able to copy out the left-overs
         * anyway. Instead force the position to stay on predefined datatypes
         * boundaries. As we allow partial predefined datatypes on the contiguous
         * case, we should be accepted by any receiver convertor.
         */
        if( CONVERTOR_SEND & convertor->flags ) {
            convertor->bConverted -= convertor->partial_length;
            convertor->partial_length = 0;
        }
    }
    *position = convertor->bConverted;
    return rc;
}

static size_t
parsec_datatype_compute_remote_size( const parsec_datatype_t* pData,
                                   const size_t* sizes )
{
    uint32_t typeMask = pData->bdt_used;
    size_t length = 0;

    if (parsec_datatype_is_predefined(pData)) {
        return sizes[pData->desc.desc->elem.common.type];
    }

    if( PARSEC_UNLIKELY(NULL == pData->ptypes) ) {
        /* Allocate and fill the array of types used in the datatype description */
        parsec_datatype_compute_ptypes( (parsec_datatype_t*)pData );
    }

    for( int i = PARSEC_DATATYPE_FIRST_TYPE; typeMask && (i < PARSEC_DATATYPE_MAX_PREDEFINED); i++ ) {
        if( typeMask & ((uint32_t)1 << i) ) {
            length += (pData->ptypes[i] * sizes[i]);
            typeMask ^= ((uint32_t)1 << i);
        }
    }
    return length;
}

/**
 * Compute the remote size. If necessary remove the homogeneous flag
 * and redirect the convertor description toward the non-optimized
 * datatype representation.
 */
size_t parsec_convertor_compute_remote_size( parsec_convertor_t* pConvertor )
{
    parsec_datatype_t* datatype = (parsec_datatype_t*)pConvertor->pDesc;
    
    pConvertor->remote_size = pConvertor->local_size;
    if( PARSEC_UNLIKELY(datatype->bdt_used & pConvertor->master->hetero_mask) ) {
        pConvertor->flags &= (~CONVERTOR_HOMOGENEOUS);
        if (!(pConvertor->flags & CONVERTOR_SEND && pConvertor->flags & PARSEC_DATATYPE_FLAG_CONTIGUOUS)) {
            pConvertor->use_desc = &(datatype->desc);
        }
        if( 0 == (pConvertor->flags & CONVERTOR_HAS_REMOTE_SIZE) ) {
            /* This is for a single datatype, we must update it with the count */
            pConvertor->remote_size = parsec_datatype_compute_remote_size(datatype,
                                                                        pConvertor->master->remote_sizes);
            pConvertor->remote_size *= pConvertor->count;
        }
    }
    pConvertor->flags |= CONVERTOR_HAS_REMOTE_SIZE;
    return pConvertor->remote_size;
}

/**
 * This macro will initialize a convertor based on a previously created
 * convertor. The idea is the move outside these function the heavy
 * selection of architecture features for the convertors. I consider
 * here that the convertor is clean, either never initialized or already
 * cleaned.
 */
#define PARSEC_CONVERTOR_PREPARE( convertor, datatype, count, pUserBuf )  \
    {                                                                   \
        convertor->local_size = count * datatype->size;                 \
        convertor->pBaseBuf   = (unsigned char*)pUserBuf;               \
        convertor->count      = count;                                  \
        convertor->pDesc      = (parsec_datatype_t*)datatype;             \
        convertor->bConverted = 0;                                      \
        convertor->use_desc   = &(datatype->opt_desc);                  \
        /* If the data is empty we just mark the convertor as           \
         * completed. With this flag set the pack and unpack functions  \
         * will not do anything.                                        \
         */                                                             \
        if( PARSEC_UNLIKELY((0 == count) || (0 == datatype->size)) ) {    \
            convertor->flags |= (PARSEC_DATATYPE_FLAG_NO_GAPS | CONVERTOR_COMPLETED | CONVERTOR_HAS_REMOTE_SIZE); \
            convertor->local_size = convertor->remote_size = 0;         \
            return PARSEC_SUCCESS;                                        \
        }                                                               \
                                                                        \
        /* Grab the datatype part of the flags */                       \
        convertor->flags     &= CONVERTOR_TYPE_MASK;                    \
        convertor->flags     |= (CONVERTOR_DATATYPE_MASK & datatype->flags); \
        convertor->flags     |= (CONVERTOR_NO_OP | CONVERTOR_HOMOGENEOUS); \
                                                                        \
        convertor->remote_size = convertor->local_size;                 \
        if( PARSEC_LIKELY(convertor->remoteArch == parsec_local_arch) ) {   \
            if( !(convertor->flags & CONVERTOR_WITH_CHECKSUM) &&        \
                ((convertor->flags & PARSEC_DATATYPE_FLAG_NO_GAPS) || \
                 ((convertor->flags & PARSEC_DATATYPE_FLAG_CONTIGUOUS) && (1 == count))) ) { \
                return PARSEC_SUCCESS;                                    \
            }                                                           \
        }                                                               \
                                                                        \
        assert( (convertor)->pDesc == (datatype) );                     \
        parsec_convertor_compute_remote_size( convertor );                \
        assert( NULL != convertor->use_desc->desc );                    \
        /* For predefined datatypes (contiguous) do nothing more */     \
        /* if checksum is enabled then always continue */               \
        if( ((convertor->flags & (CONVERTOR_WITH_CHECKSUM | PARSEC_DATATYPE_FLAG_NO_GAPS)) \
             == PARSEC_DATATYPE_FLAG_NO_GAPS) &&                          \
            ((convertor->flags & (CONVERTOR_SEND | CONVERTOR_HOMOGENEOUS)) == \
             (CONVERTOR_SEND | CONVERTOR_HOMOGENEOUS)) ) {              \
            return PARSEC_SUCCESS;                                        \
        }                                                               \
        convertor->flags &= ~CONVERTOR_NO_OP;                           \
        {                                                               \
            uint32_t required_stack_length = datatype->loops + 1;       \
                                                                        \
            if( required_stack_length > convertor->stack_size ) {       \
                assert(convertor->pStack == convertor->static_stack);   \
                convertor->stack_size = required_stack_length;          \
                convertor->pStack     = (dt_stack_t*)malloc(sizeof(dt_stack_t) * \
                                                            convertor->stack_size ); \
            }                                                           \
        }                                                               \
        parsec_convertor_create_stack_at_begining( convertor, parsec_datatype_local_sizes ); \
    }


int32_t parsec_convertor_prepare_for_recv( parsec_convertor_t* convertor,
                                         const struct parsec_datatype_t* datatype,
                                         size_t count,
                                         const void* pUserBuf )
{
    /* Here I should check that the data is not overlapping */

    convertor->flags |= CONVERTOR_RECV;
#if PARSEC_CUDA_SUPPORT
    if (!( convertor->flags & CONVERTOR_SKIP_CUDA_INIT )) {
        mca_cuda_convertor_init(convertor, pUserBuf);
    }
#endif

    assert(! (convertor->flags & CONVERTOR_SEND));
    PARSEC_CONVERTOR_PREPARE( convertor, datatype, count, pUserBuf );

#if defined(CHECKSUM)
    if( PARSEC_UNLIKELY(convertor->flags & CONVERTOR_WITH_CHECKSUM) ) {
        if( PARSEC_UNLIKELY(!(convertor->flags & CONVERTOR_HOMOGENEOUS)) ) {
            convertor->fAdvance = parsec_unpack_general_checksum;
        } else {
            if( convertor->pDesc->flags & PARSEC_DATATYPE_FLAG_CONTIGUOUS ) {
                convertor->fAdvance = parsec_unpack_homogeneous_contig_checksum;
            } else {
                convertor->fAdvance = parsec_generic_simple_unpack_checksum;
            }
        }
    } else
#endif  /* defined(CHECKSUM) */
        if( PARSEC_UNLIKELY(!(convertor->flags & CONVERTOR_HOMOGENEOUS)) ) {
            convertor->fAdvance = parsec_unpack_general;
        } else {
            if( convertor->pDesc->flags & PARSEC_DATATYPE_FLAG_CONTIGUOUS ) {
                convertor->fAdvance = parsec_unpack_homogeneous_contig;
            } else {
                convertor->fAdvance = parsec_generic_simple_unpack;
            }
        }
    return PARSEC_SUCCESS;
}


int32_t parsec_convertor_prepare_for_send( parsec_convertor_t* convertor,
                                         const struct parsec_datatype_t* datatype,
                                         size_t count,
                                         const void* pUserBuf )
{
    convertor->flags |= CONVERTOR_SEND;
#if PARSEC_CUDA_SUPPORT
    if (!( convertor->flags & CONVERTOR_SKIP_CUDA_INIT )) {
        mca_cuda_convertor_init(convertor, pUserBuf);
    }
#endif

    PARSEC_CONVERTOR_PREPARE( convertor, datatype, count, pUserBuf );

#if defined(CHECKSUM)
    if( convertor->flags & CONVERTOR_WITH_CHECKSUM ) {
        if( CONVERTOR_SEND_CONVERSION == (convertor->flags & (CONVERTOR_SEND_CONVERSION|CONVERTOR_HOMOGENEOUS)) ) {
            convertor->fAdvance = parsec_pack_general_checksum;
        } else {
            if( datatype->flags & PARSEC_DATATYPE_FLAG_CONTIGUOUS ) {
                if( ((datatype->ub - datatype->lb) == (ptrdiff_t)datatype->size)
                    || (1 >= convertor->count) )
                    convertor->fAdvance = parsec_pack_homogeneous_contig_checksum;
                else
                    convertor->fAdvance = parsec_pack_homogeneous_contig_with_gaps_checksum;
            } else {
                convertor->fAdvance = parsec_generic_simple_pack_checksum;
            }
        }
    } else
#endif  /* defined(CHECKSUM) */
        if( CONVERTOR_SEND_CONVERSION == (convertor->flags & (CONVERTOR_SEND_CONVERSION|CONVERTOR_HOMOGENEOUS)) ) {
            convertor->fAdvance = parsec_pack_general;
        } else {
            if( datatype->flags & PARSEC_DATATYPE_FLAG_CONTIGUOUS ) {
                if( ((datatype->ub - datatype->lb) == (ptrdiff_t)datatype->size)
                    || (1 >= convertor->count) )
                    convertor->fAdvance = parsec_pack_homogeneous_contig;
                else
                    convertor->fAdvance = parsec_pack_homogeneous_contig_with_gaps;
            } else {
                convertor->fAdvance = parsec_generic_simple_pack;
            }
        }
    return PARSEC_SUCCESS;
}

/*
 * These functions can be used in order to create an IDENTICAL copy of one convertor. In this
 * context IDENTICAL means that the datatype and count and all other properties of the basic
 * convertor get replicated on this new convertor. However, the references to the datatype
 * are not increased. This function take special care about the stack. If all the cases the
 * stack is created with the correct number of entries but if the copy_stack is true (!= 0)
 * then the content of the old stack is copied on the new one. The result will be a convertor
 * ready to use starting from the old position. If copy_stack is false then the convertor
 * is created with a empty stack (you have to use parsec_convertor_set_position before using it).
 */
int parsec_convertor_clone( const parsec_convertor_t* source,
                          parsec_convertor_t* destination,
                          int32_t copy_stack )
{
    destination->remoteArch        = source->remoteArch;
    destination->flags             = source->flags;
    destination->pDesc             = source->pDesc;
    destination->use_desc          = source->use_desc;
    destination->count             = source->count;
    destination->pBaseBuf          = source->pBaseBuf;
    destination->fAdvance          = source->fAdvance;
    destination->master            = source->master;
    destination->local_size        = source->local_size;
    destination->remote_size       = source->remote_size;
    /* create the stack */
    if( PARSEC_UNLIKELY(source->stack_size > DT_STATIC_STACK_SIZE) ) {
        destination->pStack = (dt_stack_t*)malloc(sizeof(dt_stack_t) * source->stack_size );
    } else {
        destination->pStack = destination->static_stack;
    }
    destination->stack_size = source->stack_size;

    /* initialize the stack */
    if( PARSEC_LIKELY(0 == copy_stack) ) {
        destination->bConverted = -1;
        destination->stack_pos  = -1;
    } else {
        memcpy( destination->pStack, source->pStack, sizeof(dt_stack_t) * (source->stack_pos+1) );
        destination->bConverted = source->bConverted;
        destination->stack_pos  = source->stack_pos;
    }
#if PARSEC_CUDA_SUPPORT
    destination->cbmemcpy   = source->cbmemcpy;
#endif
    return PARSEC_SUCCESS;
}


void parsec_convertor_dump( parsec_convertor_t* convertor )
{
    parsec_output( 0, "Convertor %p count %" PRIsize_t " stack position %u bConverted %" PRIsize_t "\n"
                 "\tlocal_size %" PRIsize_t " remote_size %" PRIsize_t " flags %X stack_size %u pending_length %" PRIsize_t "\n"
                 "\tremote_arch %u local_arch %u\n",
                 (void*)convertor,
                 convertor->count, convertor->stack_pos, convertor->bConverted,
                 convertor->local_size, convertor->remote_size,
                 convertor->flags, convertor->stack_size, convertor->partial_length,
                 convertor->remoteArch, parsec_local_arch );
    if( convertor->flags & CONVERTOR_RECV ) parsec_output( 0, "unpack ");
    if( convertor->flags & CONVERTOR_SEND ) parsec_output( 0, "pack ");
    if( convertor->flags & CONVERTOR_SEND_CONVERSION ) parsec_output( 0, "conversion ");
    if( convertor->flags & CONVERTOR_HOMOGENEOUS ) parsec_output( 0, "homogeneous " );
    else parsec_output( 0, "heterogeneous ");
    if( convertor->flags & CONVERTOR_NO_OP ) parsec_output( 0, "no_op ");
    if( convertor->flags & CONVERTOR_WITH_CHECKSUM ) parsec_output( 0, "checksum ");
    if( convertor->flags & CONVERTOR_CUDA ) parsec_output( 0, "CUDA ");
    if( convertor->flags & CONVERTOR_CUDA_ASYNC ) parsec_output( 0, "CUDA Async ");
    if( convertor->flags & CONVERTOR_COMPLETED ) parsec_output( 0, "COMPLETED ");

    parsec_datatype_dump( convertor->pDesc );
    if( !((0 == convertor->stack_pos) &&
          ((size_t)convertor->pStack[convertor->stack_pos].index > convertor->pDesc->desc.length)) ) {
        /* only if the convertor is completely initialized */
        parsec_output( 0, "Actual stack representation\n" );
        parsec_datatype_dump_stack( convertor->pStack, convertor->stack_pos,
                                  convertor->pDesc->desc.desc, convertor->pDesc->name );
    }
}


void parsec_datatype_dump_stack( const dt_stack_t* pStack, int stack_pos,
                               const union dt_elem_desc* pDesc, const char* name )
{
    parsec_output( 0, "\nStack %p stack_pos %d name %s\n", (void*)pStack, stack_pos, name );
    for( ; stack_pos >= 0; stack_pos-- ) {
        parsec_output( 0, "%d: pos %d count %" PRIsize_t " disp %ld ", stack_pos, pStack[stack_pos].index,
                     pStack[stack_pos].count, pStack[stack_pos].disp );
        if( pStack->index != -1 )
            parsec_output( 0, "\t[desc count %lu disp %ld extent %ld]\n",
                         (unsigned long)pDesc[pStack[stack_pos].index].elem.count,
                         (long)pDesc[pStack[stack_pos].index].elem.disp,
                         (long)pDesc[pStack[stack_pos].index].elem.extent );
        else
            parsec_output( 0, "\n" );
    }
    parsec_output( 0, "\n" );
}
