/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _PARSEC_DATATYPE_PROTOTYPES_H_HAS_BEEN_INCLUDED
#define _PARSEC_DATATYPE_PROTOTYPES_H_HAS_BEEN_INCLUDED

#include "parsec/parsec_config.h"
#include "parsec/datatype/parsec_datatype_config.h"


BEGIN_C_DECLS

/*
 * First the public ones
 */

PARSEC_DECLSPEC int32_t
parsec_pack_general( parsec_convertor_t* pConvertor,
                   struct iovec* iov, uint32_t* out_size,
                   size_t* max_data );
PARSEC_DECLSPEC int32_t
parsec_pack_general_checksum( parsec_convertor_t* pConvertor,
                            struct iovec* iov, uint32_t* out_size,
                            size_t* max_data );
PARSEC_DECLSPEC int32_t
parsec_unpack_general( parsec_convertor_t* pConvertor,
                     struct iovec* iov, uint32_t* out_size,
                     size_t* max_data );
PARSEC_DECLSPEC int32_t
parsec_unpack_general_checksum( parsec_convertor_t* pConvertor,
                              struct iovec* iov, uint32_t* out_size,
                              size_t* max_data );

/*
 * Now the internal functions
 */
int32_t
parsec_pack_homogeneous_contig( parsec_convertor_t* pConv,
                          struct iovec* iov, uint32_t* out_size,
                          size_t* max_data );
int32_t
parsec_pack_homogeneous_contig_checksum( parsec_convertor_t* pConv,
                                   struct iovec* iov, uint32_t* out_size,
                                   size_t* max_data );
int32_t
parsec_pack_homogeneous_contig_with_gaps( parsec_convertor_t* pConv,
                                    struct iovec* iov, uint32_t* out_size,
                                    size_t* max_data );
int32_t
parsec_pack_homogeneous_contig_with_gaps_checksum( parsec_convertor_t* pConv,
                                             struct iovec* iov, uint32_t* out_size,
                                             size_t* max_data );
int32_t
parsec_generic_simple_pack( parsec_convertor_t* pConvertor,
                          struct iovec* iov, uint32_t* out_size,
                          size_t* max_data );
int32_t
parsec_generic_simple_pack_checksum( parsec_convertor_t* pConvertor,
                                   struct iovec* iov, uint32_t* out_size,
                                   size_t* max_data );
int32_t
parsec_unpack_homogeneous_contig( parsec_convertor_t* pConv,
                                struct iovec* iov, uint32_t* out_size,
                                size_t* max_data );
int32_t
parsec_unpack_homogeneous_contig_checksum( parsec_convertor_t* pConv,
                                         struct iovec* iov, uint32_t* out_size,
                                         size_t* max_data );
int32_t
parsec_generic_simple_unpack( parsec_convertor_t* pConvertor,
                            struct iovec* iov, uint32_t* out_size,
                            size_t* max_data );
int32_t
parsec_generic_simple_unpack_checksum( parsec_convertor_t* pConvertor,
                                     struct iovec* iov, uint32_t* out_size,
                                     size_t* max_data );

END_C_DECLS

#endif  /* _PARSEC_DATATYPE_PROTOTYPES_H_HAS_BEEN_INCLUDED */
