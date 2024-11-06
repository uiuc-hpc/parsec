/*
 * Copyright (c) 2015-2019 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#ifndef _PARSEC_RECURSIVE_H_
#define _PARSEC_RECURSIVE_H_

#include "parsec/parsec_config.h"
#include "parsec/execution_stream.h"
#include "parsec/scheduling.h"
#include "parsec/mca/device/device.h"
#include "parsec/data_dist/matrix/matrix.h"
#include "parsec/parsec_stats.h"

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
    parsec_task_t* task = data->task;
    parsec_execution_stream_t *es = parsec_my_execution_stream();

#if defined(PARSEC_STATS_TC)
    /* sum time for all tasks from this recursive taskpool */
    double tp_time_execute = 0.0;
    for (uint32_t i = 0; i < tp->nb_task_classes; i++) {
        const parsec_task_class_t *tc = tp->task_classes_array[i];
        kahan_sum_t time_execute = atomic_load_explicit(&tc->time_execute,
                                                        memory_order_relaxed);
        tp_time_execute += time_execute.sum;
    }
    /* task->task_class is const, so we need to cast that away
     * this is safe, since it should always be in dynamic memory */
    atomic_kahan_sum((_Atomic(kahan_sum_t) *)&task->task_class->time_execute,
                     tp_time_execute, memory_order_relaxed, memory_order_relaxed);
#endif /* PARSEC_STATS_TC */

#if defined(PARSEC_SIM_TIME)
    /* retrieve taskpool critical path */
    kahan_sum_t sim_exec_time = atomic_load_explicit(&tp->largest_simulation_time,
                                                     memory_order_relaxed);
    /* store to task */
    task->sim_exec_time = sim_exec_time;
    /* update max time on this execution stream */
    if( es->largest_simulation_time < sim_exec_time.sum ) {
        /* atomic store to prevent tearing */
        atomic_store_explicit((_Atomic(double) *)&es->largest_simulation_time,
                              sim_exec_time.sum, memory_order_relaxed);
    }
    /* update max time for parent taskpool */
    atomic_kahan_max(&task->taskpool->largest_simulation_time, &sim_exec_time,
                     memory_order_relaxed, memory_order_relaxed);
#endif /* PARSEC_SIM_TIME */

#if defined(PARSEC_SIM_COMM)
    /* retrieve taskpool critical + comm path */
    kahan_sum_t sim_exec_comm = atomic_load_explicit(&tp->largest_simulation_comm,
                                                     memory_order_relaxed);
    /* store to task */
    task->sim_exec_comm = sim_exec_comm;
    /* update max time on this execution stream */
    if( es->largest_simulation_comm < sim_exec_comm.sum ) {
        /* atomic store to prevent tearing */
        atomic_store_explicit((_Atomic(double) *)&es->largest_simulation_comm,
                              sim_exec_comm.sum, memory_order_relaxed);
    }
    /* update max time for parent taskpool */
    atomic_kahan_max(&task->taskpool->largest_simulation_comm, &sim_exec_comm,
                     memory_order_relaxed, memory_order_relaxed);
#endif /* PARSEC_SIM_COMM */

    /* call user callback *before* we complete and release the task */
    data->callback( tp, data );
    rc = __parsec_complete_execution(es, task);

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

    /* Increase priority of recursive tasks to that of parent */
    parsec_taskpool_set_priority(tp, task->priority);

    parsec_taskpool_set_complete_callback( tp, &parsec_recursivecall_callback,
                                           (void *)cbdata );

    /* set initial critical path time for recursive taskpool */
#if defined(PARSEC_SIM_TIME)
    tp->initial_simulation_time = task->sim_exec_time;
#endif /* PARSEC_SIM_TIME */

    /* set initial critical path + comm time for recursive taskpool */
#if defined(PARSEC_SIM_COMM)
    tp->initial_simulation_comm = task->sim_exec_comm;
#endif /* PARSEC_SIM_COMM */

    parsec_context_add_taskpool( task->taskpool->context, tp );

    return -1;
}

#endif /* _PARSEC_RECURSIVE_H_ */
