/*
 * Copyright (c) 2015-2019 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#ifndef _PARSEC_RECURSIVE_H_
#define _PARSEC_RECURSIVE_H_

#include "parsec/execution_stream.h"
#include "parsec/scheduling.h"
#include "parsec/mca/device/device.h"
#include "parsec/data_dist/matrix/matrix.h"

typedef struct parsec_recursive_cb_data_s parsec_recursive_cb_data_t;
typedef void (*parsec_recursive_callback)(parsec_taskpool_t*, const parsec_recursive_cb_data_t* );

typedef struct parsec_recursive_cb_data_s {
    parsec_task_t                *task;
    parsec_recursive_callback     callback;
    int nbdesc;
    parsec_data_collection_t      *desc[1];
} parsec_recursive_cb_data_t;

static inline int parsec_recursivecall_callback(parsec_taskpool_t* tp, void* cb_data)
{
    int i, rc = 0;
    parsec_recursive_cb_data_t* data = (parsec_recursive_cb_data_t*)cb_data;
    parsec_execution_stream_t *es = parsec_my_execution_stream();

    /* call user callback *before* we complete and release the task */
    data->callback( tp, data );
    rc = __parsec_complete_execution(es, data->task);

    for( i = 0; i < data->nbdesc; i++ ) {
        parsec_tiled_matrix_dc_destroy( (parsec_tiled_matrix_dc_t*)(data->desc[i]) );
        free( data->desc[i] );
    }
    free(data);

    return rc;
}

static inline int
parsec_recursivecall( parsec_task_t                *task,
                      parsec_taskpool_t            *tp,
                      parsec_recursive_callback     callback,
                      int nbdesc,
                      ... )
{
    parsec_recursive_cb_data_t *cbdata = NULL;
    va_list ap;

    /* Set mask to be used only on CPU */
    parsec_mca_device_taskpool_restrict( tp, PARSEC_DEV_CPU );

    /* Callback */
    cbdata = (parsec_recursive_cb_data_t *) malloc( sizeof(parsec_recursive_cb_data_t) + (nbdesc-1)*sizeof(parsec_data_collection_t*));
    cbdata->task     = task;
    cbdata->callback = callback;
    cbdata->nbdesc   = nbdesc;

    /* Get descriptors */
    va_start(ap, nbdesc);
    for(int i = 0; i < nbdesc; i++ ) {
        cbdata->desc[i] = va_arg(ap, parsec_data_collection_t *);
    }
    va_end(ap);

    parsec_taskpool_set_complete_callback( tp, &parsec_recursivecall_callback,
                                           (void *)cbdata );

    parsec_context_add_taskpool( task->taskpool->context, tp );

    return -1;
}

#endif /* _PARSEC_RECURSIVE_H_ */
