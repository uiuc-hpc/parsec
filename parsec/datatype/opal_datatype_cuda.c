/*
 * Copyright (c) 2011-2014 NVIDIA Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "parsec_config.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>

#include "parsec/align.h"
#include "parsec/util/output.h"
#include "parsec/datatype/parsec_convertor.h"
#include "parsec/datatype/parsec_datatype_cuda.h"

static bool initialized = false;
int parsec_cuda_verbose = 0;
static int parsec_cuda_enabled = 0; /* Starts out disabled */
static int parsec_cuda_output = 0;
static void parsec_cuda_support_init(void);
static int (*common_cuda_initialization_function)(parsec_common_cuda_function_table_t *) = NULL;
static parsec_common_cuda_function_table_t ftable;

/* This function allows the common cuda code to register an
 * initialization function that gets called the first time an attempt
 * is made to send or receive a GPU pointer.  This allows us to delay
 * some CUDA initialization until after MPI_Init().
 */
void parsec_cuda_add_initialization_function(int (*fptr)(parsec_common_cuda_function_table_t *)) {
    common_cuda_initialization_function = fptr;
}

/**
 * This function is called when a convertor is instantiated.  It has to call
 * the parsec_cuda_support_init() function once to figure out if CUDA support
 * is enabled or not.  If CUDA is not enabled, then short circuit out
 * for all future calls.
 */
void mca_cuda_convertor_init(parsec_convertor_t* convertor, const void *pUserBuf)
{
    /* Only do the initialization on the first GPU access */
    if (!initialized) {
        parsec_cuda_support_init();
    }

    /* This is needed to handle case where convertor is not fully initialized
     * like when trying to do a sendi with convertor on the statck */
    convertor->cbmemcpy = (memcpy_fct_t)&parsec_cuda_memcpy;

    /* If not enabled, then nothing else to do */
    if (!parsec_cuda_enabled) {
        return;
    }

    if (ftable.gpu_is_gpu_buffer(pUserBuf, convertor)) {
        convertor->flags |= CONVERTOR_CUDA;
    }
}

/* Checks the type of pointer
 *
 * @param dest   One pointer to check
 * @param source Another pointer to check
 */
bool parsec_cuda_check_bufs(char *dest, char *src)
{
    /* Only do the initialization on the first GPU access */
    if (!initialized) {
        parsec_cuda_support_init();
    }

    if (!parsec_cuda_enabled) {
        return false;
    }

    if (ftable.gpu_is_gpu_buffer(dest, NULL) || ftable.gpu_is_gpu_buffer(src, NULL)) {
        return true;
    } else {
        return false;
    }
}

/*
 * With CUDA enabled, all contiguous copies will pass through this function.
 * Therefore, the first check is to see if the convertor is a GPU buffer.
 * Note that if there is an error with any of the CUDA calls, the program
 * aborts as there is no recovering.
 */

/* Checks the type of pointer
 *
 * @param buf   check one pointer providing a convertor.
 *  Provides aditional information, e.g. managed vs. unmanaged GPU buffer
 */
bool  parsec_cuda_check_one_buf(char *buf, parsec_convertor_t *convertor )
{
    /* Only do the initialization on the first GPU access */
    if (!initialized) {
        parsec_cuda_support_init();
    }

    if (!parsec_cuda_enabled) {
        return false;
    }

    return ( ftable.gpu_is_gpu_buffer(buf, convertor));
}

/*
 * With CUDA enabled, all contiguous copies will pass through this function.
 * Therefore, the first check is to see if the convertor is a GPU buffer.
 * Note that if there is an error with any of the CUDA calls, the program
 * aborts as there is no recovering.
 */

void *parsec_cuda_memcpy(void *dest, const void *src, size_t size, parsec_convertor_t* convertor)
{
    int res;

    if (!(convertor->flags & CONVERTOR_CUDA)) {
        return memcpy(dest, src, size);
    }

    if (convertor->flags & CONVERTOR_CUDA_ASYNC) {
        res = ftable.gpu_cu_memcpy_async(dest, (void *)src, size, convertor);
    } else {
        res = ftable.gpu_cu_memcpy(dest, (void *)src, size);
    }

    if (res != 0) {
        parsec_output(0, "CUDA: Error in cuMemcpy: res=%d, dest=%p, src=%p, size=%d",
                    res, dest, src, (int)size);
        abort();
    } else {
        return dest;
    }
}

/*
 * This function is needed in cases where we do not have contiguous
 * datatypes.  The current code has macros that cannot handle a convertor
 * argument to the memcpy call.
 */
void *parsec_cuda_memcpy_sync(void *dest, const void *src, size_t size)
{
    int res;
    res = ftable.gpu_cu_memcpy(dest, src, size);
    if (res != 0) {
        parsec_output(0, "CUDA: Error in cuMemcpy: res=%d, dest=%p, src=%p, size=%d",
                    res, dest, src, (int)size);
        abort();
    } else {
        return dest;
    }
}

/*
 * In some cases, need an implementation of memmove.  This is not fast, but
 * it is not often needed.
 */
void *parsec_cuda_memmove(void *dest, void *src, size_t size)
{
    int res;

    res = ftable.gpu_memmove(dest, src, size);
    if(res != 0){
        parsec_output(0, "CUDA: Error in gpu memmove: res=%d, dest=%p, src=%p, size=%d",
                    res, dest, src, (int)size);
        abort();
    }
    return dest;
}

/**
 * This function gets called once to check if the program is running in a cuda
 * environment.
 */
static void parsec_cuda_support_init(void)
{
    if (initialized) {
        return;
    }

    /* Set different levels of verbosity in the cuda related code. */
    parsec_cuda_output = parsec_output_open(NULL);
    parsec_output_set_verbosity(parsec_cuda_output, parsec_cuda_verbose);

    /* Callback into the common cuda initialization routine. This is only
     * set if some work had been done already in the common cuda code.*/
    if (NULL != common_cuda_initialization_function) {
        if (0 == common_cuda_initialization_function(&ftable)) {
            parsec_cuda_enabled = 1;
        }
    }

    if (1 == parsec_cuda_enabled) {
        parsec_output_verbose(10, parsec_cuda_output,
                            "CUDA: enabled successfully, CUDA device pointers will work");
    } else {
        parsec_output_verbose(10, parsec_cuda_output,
                            "CUDA: not enabled, CUDA device pointers will not work");
    }

    initialized = true;
}

/**
 * Tell the convertor that copies will be asynchronous CUDA copies.  The
 * flags are cleared when the convertor is reinitialized.
 */
void parsec_cuda_set_copy_function_async(parsec_convertor_t* convertor, void *stream)
{
    convertor->flags |= CONVERTOR_CUDA_ASYNC;
    convertor->stream = stream;
}
