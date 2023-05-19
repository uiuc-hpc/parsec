#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <inttypes.h>
#include <time.h>
#include <pthread.h>

#include "parsec/parsec_config.h"
#include "parsec/parsec_internal.h"
#include "parsec/execution_stream.h"
#include "parsec/parsec_comm_engine.h"
#include "parsec/parsec_stats.h"
#include "parsec/vpmap.h"
#include "parsec/utils/mca_param.h"

#ifndef NSEC_PER_SEC
#define NSEC_PER_SEC (1000000000LL)
#endif

static inline void timespec_add_ns(struct timespec *ts, long nsec)
{
    /* compute b + a */
    ts->tv_nsec += nsec;
    if (ts->tv_nsec >= NSEC_PER_SEC) {
        ts->tv_nsec -= NSEC_PER_SEC;
        ts->tv_sec += 1;
    }
}

typedef struct {
    double time_start;
    double prior_execute;
    double prior_select;
    double prior_wait;
    FILE   *outfile;
} pgs_config_t;

extern pgs_thrd_info_t pgs_thrd = {
    .mtx = PTHREAD_MUTEX_INITIALIZER,
    .status = PGS_RESET,
    .waitns = 1000000L,
    .filename = NULL,
    .context = NULL,
    .enable = false,
    .sched = 0,
    .exec = 0,
    .idle = 0,
};

static inline void parsec_graph_stat_record(const parsec_context_t* context,
                                            pgs_config_t *config,
                                            pgs_status_t status)
{
    if (status != PGS_CONTINUE)
        return;

    double time_execute = 0.0;
    double time_select  = 0.0;
    double time_wait = parsec_stat_time(&parsec_stat_clock_model) - config->time_start;
    size_t tasks_sched = atomic_load_explicit(&pgs_thrd.sched, memory_order_acquire);
    size_t tasks_exec  = atomic_load_explicit(&pgs_thrd.exec,  memory_order_acquire);
    int threads_idle   = atomic_load_explicit(&pgs_thrd.idle,  memory_order_acquire);
    for (int32_t vpid = 0; vpid < context->nb_vp; vpid++) {
        parsec_vp_t *vp = context->virtual_processes[vpid];
        for (int32_t thid = 0; thid < vp->nb_cores; thid++) {
            parsec_execution_stream_t *es = vp->execution_streams[thid];
            /* load sums atomically */
            time_execute += atomic_load_explicit((_Atomic(double) *)&es->time.execute.sum, memory_order_relaxed);
            time_select  += atomic_load_explicit((_Atomic(double) *)&es->time.select.sum,  memory_order_relaxed);
        }
    }
    double diff_execute = time_execute - config->prior_execute;
    double diff_select  = time_select  - config->prior_select;
    double diff_wait    = time_wait    - config->prior_wait;
    /* diff_wait is on single thread, we need to multiply by all threads */
    diff_wait *= vpmap_get_nb_total_threads();
    fprintf(config->outfile, "%f\t%f\t%f\t%f\t%zu\t%zu\t%d\n",
            time_wait, diff_execute, diff_select, diff_wait,
            tasks_sched, tasks_exec, threads_idle);
    config->prior_execute = time_execute;
    config->prior_select  = time_select;
    config->prior_wait    = time_wait;
}

static inline void parsec_graph_stat_continue(void)
{
    int ret = 0;
    struct timespec timeout;
    /* get current time */
    clock_gettime(CLOCK_MONOTONIC, &timeout);
    /* add timeout to current time */
    timespec_add_ns(&timeout, pgs_thrd.waitns);
    /* wait until we're signaled or time out */
    do {
        ret = pthread_cond_timedwait(&pgs_thrd.cond, &pgs_thrd.mtx, &timeout);
        /* check for spurious wakeup: no timeout & no cond change */
    } while (ret != ETIMEDOUT && pgs_thrd.status == PGS_CONTINUE);
}

static void * parsec_graph_stat_thread(void *arg)
{
    _Bool stop = false;
    pgs_status_t prior_status;
    pgs_config_t pgs_config;

    pgs_config.outfile = fopen(pgs_thrd.filename, "w");

    fprintf(pgs_config.outfile,
            "Time\tExecution\tSelect\tWait\tSched\tExec\tIdle\n");

    /* lock mutex */
    pthread_mutex_lock(&pgs_thrd.mtx);
    /* set thread status */
    pgs_thrd.status = PGS_PAUSE;
    prior_status    = PGS_PAUSE;
    /* signal init */
    pthread_cond_signal(&pgs_thrd.cond);
    /* NOTE - we don't unlock mutex here
     * that will happen in pthread_cond_wait, since we start in PGS_PAUSE */

    while (!stop) {
        switch (pgs_thrd.status) {
        case PGS_CONTINUE:
            /* always record stats for current epoch */
            parsec_graph_stat_record(pgs_thrd.context, &pgs_config,
                                     PGS_CONTINUE);
            prior_status = PGS_CONTINUE;
            /* wait for status change or timeout */
            parsec_graph_stat_continue();
            break;
        case PGS_PAUSE:
            /* record stats for last epoch, if necessary */
            parsec_graph_stat_record(pgs_thrd.context, &pgs_config,
                                     prior_status);
            prior_status = PGS_PAUSE;
            /* reset counters
             * other threads might increment these before we get to reset them,
             * so do this here at end of prior epoch
             * instead of in PGS_RESET at beginning of next epoch */
            atomic_store_explicit(&pgs_thrd.sched, 0, memory_order_release);
            atomic_store_explicit(&pgs_thrd.exec,  0, memory_order_release);
            atomic_store_explicit(&pgs_thrd.idle,  0, memory_order_release);
            /* wait until we're signaled */
            pthread_cond_wait(&pgs_thrd.cond, &pgs_thrd.mtx);
            break;
        case  PGS_STOP:
            /* record stats for last epoch, if necessary */
            parsec_graph_stat_record(pgs_thrd.context, &pgs_config,
                                     prior_status);
            prior_status = PGS_STOP;
            /* stop the thread */
            stop = true;
            break;
        case PGS_RESET:
            /* record stats for last epoch, if necessary */
            parsec_graph_stat_record(pgs_thrd.context, &pgs_config,
                                     prior_status);
            prior_status = PGS_RESET;
            /* reset internal structures */
            pgs_config.time_start = parsec_stat_time(&parsec_stat_clock_model);
            pgs_config.prior_execute = 0.0;
            pgs_config.prior_select  = 0.0;
            pgs_config.prior_wait    = 0.0;
            /* record first (zero) measurement */
            fprintf(config->outfile, "%f\t%f\t%f\t%f\t%zu\t%zu\t%d\n",
                    0.0, 0.0, 0.0, 0.0, 0, 0, vpmap_get_nb_total_threads());
            /* set status to PGS_CONTINUE to start measurement */
            pgs_thrd.status = PGS_CONTINUE;
            /* wait for status change or timeout */
            parsec_graph_stat_continue();
            break;
        }
    }
    pthread_mutex_unlock(&pgs_thrd.mtx);

    fclose(pgs_config.outfile);
    return NULL;
}

void parsec_graph_stat_init(const parsec_context_t* context)
{
    char *filename = pgs_thrd.filename;
    parsec_mca_param_reg_sizet_name("stat", "waitns",
                                    "Stat graph collection interval",
                                    false, false,
                                    pgs_thrd.waitns, &pgs_thrd.waitns);
    parsec_mca_param_reg_string_name("stat", "file",
                                     "Stat graph base file name (default disabled)",
                                     false, false,
                                     filename, &filename);

    /* only start thread if explicitly asked to do so */
    if (filename) {
        /* append rank to filename */
        asprintf(&pgs_thrd.filename, "%s-%d.tsv", filename, context->my_rank);
        pgs_thrd.context = context;

        /* we use CLOCK_MONOTONIC_RAW for timing,
         * so we need to the condition variable to use the same clock source
         * for timed waiting */
        pthread_condattr_t condattr;
        pthread_condattr_init(&condattr);
        /* pthread_condattr_setclock doesn't support CLOCK_MONOTONIC_RAW :(
         * use CLOCK_MONOTONIC instead */
        pthread_condattr_setclock(&condattr, CLOCK_MONOTONIC);
        pthread_cond_init(&pgs_thrd.cond, &condattr);
        pthread_condattr_destroy(&condattr);

        /* lock mutex */
        pthread_mutex_lock(&pgs_thrd.mtx);
        /* set thread status */
        pgs_thrd.status = PGS_RESET;
        /* create graph stat thread */
        pthread_create(&pgs_thrd.thread, NULL, parsec_graph_stat_thread, NULL);
        /* wait until thread signals us it's started and waiting */
        while (pgs_thrd.status != PGS_RESET) {
            pthread_cond_wait(&pgs_thrd.cond, &pgs_thrd.mtx);
        }
        /* unlock mutex */
        pthread_mutex_unlock(&pgs_thrd.mtx);
    }
}

void parsec_graph_stat_fini(void)
{
    /* check if we even started the thread */
    if (pgs_thrd.filename) {
        /* wait until we acquire the mutex (thread is asleep, waiting on cond) */
        pthread_mutex_lock(&pgs_thrd.mtx);
        /* set thread status */
        pgs_thrd.status = PGS_STOP;
        /* signal thread to wake up */
        pthread_cond_signal(&pgs_thrd.cond);
        /* unlock mutex and let thread stop itself */
        pthread_mutex_unlock(&pgs_thrd.mtx);
        /* wait for thread to stop and join with thread */
        pthread_join(pgs_thrd.thread, NULL);

        /* free resources */
        pthread_cond_destroy(&pgs_thrd.cond);
        free(pgs_thrd.filename);
        pgs_thrd.filename = NULL;
    }
}

void parsec_graph_stat_start(void)
{
    /* check if we even started the thread and are enabled */
    if (pgs_thrd.filename && pgs_thrd.enable) {
        /* wait until we acquire the mutex (thread is asleep, waiting on cond) */
        pthread_mutex_lock(&pgs_thrd.mtx);
        /* set thread status */
        pgs_thrd.status = PGS_RESET;
        /* signal thread to wake up */
        pthread_cond_signal(&pgs_thrd.cond);
        /* unlock mutex and let thread stop itself */
        pthread_mutex_unlock(&pgs_thrd.mtx);
    }
}

void parsec_graph_stat_end(void)
{
    /* check if we even started the thread */
    if (pgs_thrd.filename) {
#if defined(DISTRIBUTED)
        /* wait until all nodes are ending this collection epoch */
        parsec_ce.sync(&parsec_ce);
#endif /* DISTRIBUTED */
        /* wait until we acquire the mutex (thread is asleep, waiting on cond) */
        pthread_mutex_lock(&pgs_thrd.mtx);
        /* set thread status */
        pgs_thrd.status = PGS_PAUSE;
        /* signal thread to wake up */
        pthread_cond_signal(&pgs_thrd.cond);
        /* unlock mutex and let thread stop itself */
        pthread_mutex_unlock(&pgs_thrd.mtx);
    }
}
