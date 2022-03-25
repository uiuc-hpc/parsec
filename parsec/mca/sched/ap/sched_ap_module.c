/**
 * Copyright (c) 2013-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "parsec/parsec_config.h"
#include "parsec/parsec_internal.h"
#include "parsec/utils/debug.h"
#include "parsec/mca/sched/sched.h"
#include "parsec/mca/sched/ap/sched_ap.h"
#include "parsec/class/dequeue.h"
#include "parsec/mca/pins/pins.h"
#include "parsec/mca/sched/sched_local_queues_utils.h"
#include "parsec/papi_sde.h"

/**
 * Module functions
 */
static int sched_ap_install(parsec_context_t* master);
static int sched_ap_schedule(parsec_execution_stream_t* es,
                             parsec_task_t* new_context,
                             int32_t distance);
static parsec_task_t*
sched_ap_select(parsec_execution_stream_t *es,
                int32_t* distance);
static int flow_ap_init(parsec_execution_stream_t* es, struct parsec_barrier_t* barrier);
static void sched_ap_remove(parsec_context_t* master);

const parsec_sched_module_t parsec_sched_ap_module = {
    &parsec_sched_ap_component,
    {
        sched_ap_install,
        flow_ap_init,
        sched_ap_schedule,
        sched_ap_select,
        NULL,
        sched_ap_remove
    }
};

#define LOCAL_SCHED_OBJECT(eu_context) ((parsec_mca_sched_list_local_counter_t*)(eu_context)->scheduler_object)

static int sched_ap_install( parsec_context_t *master )
{
    (void)master;
    return 0;
}

static int flow_ap_init(parsec_execution_stream_t* es, struct parsec_barrier_t* barrier)
{
    parsec_vp_t *vp = es->virtual_process;
    parsec_mca_sched_list_local_counter_t *sl;

    if (es == vp->execution_streams[0]) {
        sl = parsec_mca_sched_allocate_list_local_counter( NULL );
        es->scheduler_object = sl;
    }
    
    parsec_barrier_wait(barrier);

    if( es != vp->execution_streams[0] ) {
        sl = parsec_mca_sched_allocate_list_local_counter( LOCAL_SCHED_OBJECT(vp->execution_streams[0]) );
        es->scheduler_object = sl;
    }
    
#if defined(PARSEC_PAPI_SDE)
    if( es->th_id ) {
        char event_name[PARSEC_PAPI_SDE_MAX_COUNTER_NAME_LEN];
        snprintf(event_name, PARSEC_PAPI_SDE_MAX_COUNTER_NAME_LEN,
                 "SCHEDULER::PENDING_TASKS::QUEUE=%d::SCHED=AP", es->virtual_process->vp_id);
        parsec_papi_sde_register_fp_counter(event_name, PAPI_SDE_RO|PAPI_SDE_INSTANT, PAPI_SDE_int, 
                                            (papi_sde_fptr_t)parsec_mca_sched_list_local_counter_length, es->virtual_process);
        parsec_papi_sde_add_counter_to_group(event_name, "SCHEDULER::PENDING_TASKS", PAPI_SDE_SUM);
        parsec_papi_sde_add_counter_to_group(event_name, "SCHEDULER::PENDING_TASKS::SCHED=AP", PAPI_SDE_SUM);
    }
#endif

    return 0;
}

static parsec_task_t*
sched_ap_select(parsec_execution_stream_t *es,
                int32_t* distance)
{
    parsec_mca_sched_list_local_counter_t *sl = LOCAL_SCHED_OBJECT(es);
    parsec_task_t * context = parsec_mca_sched_list_local_counter_pop_front(sl);
    *distance = 0;
    return context;
}

static int sched_ap_schedule(parsec_execution_stream_t* es,
                             parsec_task_t* new_context,
                             int32_t distance)
{
    parsec_mca_sched_list_local_counter_t *sl = LOCAL_SCHED_OBJECT(es);
#if defined(PARSEC_DEBUG_NOISIER)
    parsec_list_item_t *it = (parsec_list_item_t*)new_context;
    char tmp[MAX_TASK_STRLEN];
    do {
        PARSEC_DEBUG_VERBOSE(20, parsec_debug_output, "AP:\t Pushing task %s",
                parsec_task_snprintf(tmp, MAX_TASK_STRLEN, (parsec_task_t*)it));
        it = (parsec_list_item_t*)((parsec_list_item_t*)it)->list_next;
    } while( it != (parsec_list_item_t*)new_context );
#endif
    parsec_mca_sched_list_local_counter_chain_sorted(sl, new_context, parsec_execution_context_priority_comparator);
    (void)distance;
    return 0;
}

static void sched_ap_remove( parsec_context_t *master )
{
    int p, t;
    parsec_vp_t *vp;
    parsec_execution_stream_t *es;
    parsec_mca_sched_list_local_counter_t *sl;
    for(p = 0; p < master->nb_vp; p++) {
        vp = master->virtual_processes[p];
        for(t = 0; t < vp->nb_cores; t++) {
            es = vp->execution_streams[t];
            sl = LOCAL_SCHED_OBJECT(es);
            parsec_mca_sched_free_list_local_counter(sl);
            es->scheduler_object = NULL;
        }
        PARSEC_PAPI_SDE_UNREGISTER_COUNTER("SCHEDULER::PENDING_TASKS::QUEUE=%d::SCHED=AP", p);
    }
    PARSEC_PAPI_SDE_UNREGISTER_COUNTER("SCHEDULER::PENDING_TASKS::SCHED=AP");
}
