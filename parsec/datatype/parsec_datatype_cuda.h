/*
 * Copyright (c) 2011-2014 NVIDIA Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _PARSEC_DATATYPE_CUDA_H
#define _PARSEC_DATATYPE_CUDA_H

/* Structure to hold CUDA support functions that gets filled in when the
 * common cuda code is initialized.  This removes any dependency on <cuda.h>
 * in the parsec cuda datatype code. */
struct parsec_common_cuda_function_table {
    int (*gpu_is_gpu_buffer)(const void*, parsec_convertor_t*);
    int (*gpu_cu_memcpy_async)(void*, const void*, size_t, parsec_convertor_t*);
    int (*gpu_cu_memcpy)(void*, const void*, size_t);
    int (*gpu_memmove)(void*, void*, size_t);
};
typedef struct parsec_common_cuda_function_table parsec_common_cuda_function_table_t;

void mca_cuda_convertor_init(parsec_convertor_t* convertor, const void *pUserBuf);
bool parsec_cuda_check_bufs(char *dest, char *src);
bool parsec_cuda_check_one_buf(char *buf, parsec_convertor_t *convertor );
void* parsec_cuda_memcpy(void * dest, const void * src, size_t size, parsec_convertor_t* convertor);
void* parsec_cuda_memcpy_sync(void * dest, const void * src, size_t size);
void* parsec_cuda_memmove(void * dest, void * src, size_t size);
void parsec_cuda_add_initialization_function(int (*fptr)(parsec_common_cuda_function_table_t *));
void parsec_cuda_set_copy_function_async(parsec_convertor_t* convertor, void *stream);

#endif
