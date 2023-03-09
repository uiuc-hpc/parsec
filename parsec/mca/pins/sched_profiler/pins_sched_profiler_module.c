/*
 * Copyright (c) 2012-2019 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#include <errno.h>
#include <stdio.h>
#include "parsec/parsec_config.h"
#include "parsec/mca/pins/pins.h"
#include "pins_sched_profiler.h"
#include "parsec/profiling.h"
#include "parsec/execution_stream.h"
#include "parsec/interfaces/superscalar/insert_function_internal.h"

static int select_trace_keyin;
static int select_trace_keyout;

typedef struct sched_profiler_info_s {
    uint32_t tpid;
    uint32_t tcid;
    uint64_t tid;
} sched_profiler_info_t;
static const char *sched_profiler_info_str = "tpid{uint32_t};tcid{uint32_t};tid{uint64_t}";

/* init functions */
static void pins_init_sched_profiler(parsec_context_t *master_context);
static void pins_fini_sched_profiler(parsec_context_t *master_context);
static void pins_thread_init_sched_profiler(struct parsec_execution_stream_s * es);
static void pins_thread_fini_sched_profiler(struct parsec_execution_stream_s * es);

/* PINS callbacks */
static void sched_profiler_select_begin(struct parsec_execution_stream_s*   es,
                                        struct parsec_task_s*               task,
                                        struct parsec_pins_next_callback_s* cb_data);
static void sched_profiler_select_end(struct parsec_execution_stream_s*   es,
                                      struct parsec_task_s*               task,
                                      struct parsec_pins_next_callback_s* cb_data);

const parsec_pins_module_t parsec_pins_sched_profiler_module = {
    &parsec_pins_sched_profiler_component,
    {
        pins_init_sched_profiler,
        pins_fini_sched_profiler,
        NULL,
        NULL,
        pins_thread_init_sched_profiler,
        pins_thread_fini_sched_profiler
    },
    { NULL }
};

static void pins_init_sched_profiler(parsec_context_t *master_context)
{
    (void)master_context;
    parsec_profiling_add_dictionary_keyword("SELECT", "fill:#FF0000",
                                           sizeof(sched_profiler_info_t),
                                           sched_profiler_info_str,
                                           &select_trace_keyin,
                                           &select_trace_keyout);
}

static void pins_fini_sched_profiler(parsec_context_t *master_context)
{
    (void)master_context;
}

static void pins_thread_init_sched_profiler(struct parsec_execution_stream_s * es)
{
    parsec_pins_next_callback_t* event_cb;
    event_cb = (parsec_pins_next_callback_t*)malloc(sizeof(parsec_pins_next_callback_t));
    PARSEC_PINS_REGISTER(es, SELECT_BEGIN, sched_profiler_select_begin, event_cb);
    event_cb = (parsec_pins_next_callback_t*)malloc(sizeof(parsec_pins_next_callback_t));
    PARSEC_PINS_REGISTER(es, SELECT_END, sched_profiler_select_end, event_cb);
}

static void pins_thread_fini_sched_profiler(struct parsec_execution_stream_s * es)
{
    parsec_pins_next_callback_t* event_cb;
    PARSEC_PINS_UNREGISTER(es, SELECT_BEGIN, sched_profiler_select_begin, &event_cb);
    free(event_cb);
    PARSEC_PINS_UNREGISTER(es, SELECT_END, sched_profiler_select_end, &event_cb);
    free(event_cb);
}

/*
 PINS CALLBACKS
 */

static void
sched_profiler_select_begin(struct parsec_execution_stream_s*   es,
                            struct parsec_task_s*               task,
                            struct parsec_pins_next_callback_s* cb_data)
{
    PARSEC_PROFILING_TRACE(es->es_profile, select_trace_keyin,
                           0, PROFILE_OBJECT_ID_NULL, NULL);
    (void)cb_data;(void)task;
}

static void
sched_profiler_select_end(struct parsec_execution_stream_s*   es,
                          struct parsec_task_s*               task,
                          struct parsec_pins_next_callback_s* cb_data)
{
    sched_profiler_info_t info = { .tpid = 0, .tcid = 0, .tid = 0 };

    /* if select() found a task, get tpid, tcid, and tid */
    if (NULL != task &&
        task->task_class->task_class_id < task->taskpool->nb_task_classes) {
        info.tpid = task->taskpool->taskpool_id;
        info.tcid = task->task_class->task_class_id;
        info.tid  = task->task_class->key_functions->key_hash(
                            task->task_class->make_key( task->taskpool, task->locals ), NULL);
    }
    PARSEC_PROFILING_TRACE(es->es_profile, select_trace_keyout,
                           0, PROFILE_OBJECT_ID_NULL, &info);
    (void)cb_data;
}

