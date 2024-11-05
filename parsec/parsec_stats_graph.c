#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <inttypes.h>
#include <math.h>
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

pgs_thrd_info_t pgs_thrd = {
    .mtx = PTHREAD_MUTEX_INITIALIZER,
    .status = PGS_RESET,
    .waitns = 1000000L,
    .filename = NULL,
    .context = NULL,
    .bfmt = 0,
    .enable = false,
    .data = {
        .known     = 0,
        .ready     = 0,
        .executed  = 0,
        .completed = 0,
        .retired   = 0,
        .idle      = 0,
    },
};

typedef struct {
    double time_current;
    double time_execution;
    double time_select;
    double time_wait;
    size_t tasks_known;
    size_t tasks_ready;
    size_t tasks_executed;
    size_t tasks_completed;
    size_t tasks_retired;
    int threads_idle;
#if defined(PARSEC_SIM_TIME)
    double sim_path;
#endif /* PARSEC_SIM_TIME */
} pgs_record_t;

static inline void parsec_graph_stat_print_header(FILE *outfile, _Bool bfmt)
{
    /* always print tab-separated header */
    const char *header =
        "Time"
        "\tExecution\tSelect\tWait"
        "\tKnown\tReady\tExecuted\tCompleted\tRetired"
        "\tIdle"
#if defined(PARSEC_SIM_TIME)
        "\tSimPath"
#endif /* PARSEC_SIM_TIME */
        "\n";
    fputs(header, outfile);
    if (bfmt) {
        /* in the binary format, also print conversion format string */
        const char *pgs_record_format =
            "d"     /* time_current */
            "ddd"   /* time: execution, select, wait */
            "NNNNN" /* tasks: known, ready, executed, completed, retired */
            "i"     /* threads_idle */
#if defined(PARSEC_SIM_TIME)
            "d"     /* sim_path */
#endif /* PARSEC_SIM_TIME */
            "\n";   /* line-end delimiter */
        fputs(pgs_record_format, outfile);
    }
}

static inline void parsec_graph_stat_print_record(FILE *outfile, _Bool bfmt,
                                                  pgs_record_t *rcd)
{
    if (bfmt) {
        /* in the binary format, just write the record to the file as bytes */
        fwrite(rcd, sizeof(pgs_record_t), 1, outfile);
    } else {
        /* write as formated tab-separated values */
        const char *format =
            "%f"                        /* time_current */
            "\t%f\t%f\t%f"              /* time: execution, select, wait */
            "\t%zu\t%zu\t%zu\t%zu\t%zu" /* tasks: known, ready, executed, completed, retired */
            "\t%d"                      /* threads_idle */
#if defined(PARSEC_SIM_TIME)
            "\t%f"                      /* sim_path */
#endif /* PARSEC_SIM_TIME */
            "\n";
        fprintf(outfile, format
                , rcd->time_current
                , rcd->time_execution, rcd->time_select, rcd->time_wait
                , rcd->tasks_known, rcd->tasks_ready, rcd->tasks_executed,
                                    rcd->tasks_completed, rcd->tasks_retired
                , rcd->threads_idle
#if defined(PARSEC_SIM_TIME)
                , rcd->sim_path
#endif /* PARSEC_SIM_TIME */
               );
    }
}

static inline void parsec_graph_stat_record(const parsec_context_t* context,
                                            double time_start, FILE *outfile,
                                            pgs_status_t status)
{
    pgs_record_t rcd;

    if (status != PGS_CONTINUE)
        return;

    rcd.time_current    = parsec_stat_time(&parsec_stat_clock_model) - time_start;
    rcd.time_execution  = 0.0;
    rcd.time_select     = 0.0;
    /* time_current is on single thread, we need to multiply by all threads */
    rcd.time_wait       = rcd.time_current * vpmap_get_nb_total_threads();
    /* load task info */
    rcd.tasks_known     = atomic_load_explicit(&pgs_thrd.data.known,     memory_order_acquire);
    rcd.tasks_ready     = atomic_load_explicit(&pgs_thrd.data.ready,     memory_order_acquire);
    rcd.tasks_executed  = atomic_load_explicit(&pgs_thrd.data.executed,  memory_order_acquire);
    rcd.tasks_completed = atomic_load_explicit(&pgs_thrd.data.completed, memory_order_acquire);
    rcd.tasks_retired   = atomic_load_explicit(&pgs_thrd.data.retired,   memory_order_acquire);
    rcd.threads_idle    = atomic_load_explicit(&pgs_thrd.data.idle,      memory_order_acquire);
#if defined(PARSEC_SIM_TIME)
    rcd.sim_path        = 0.0;
#endif /* PARSEC_SIM_TIME */

    for (int32_t vpid = 0; vpid < context->nb_vp; vpid++) {
        parsec_vp_t *vp = context->virtual_processes[vpid];
        for (int32_t thid = 0; thid < vp->nb_cores; thid++) {
            parsec_execution_stream_t *es = vp->execution_streams[thid];
            /* load sums atomically */
            rcd.time_execution += atomic_load_explicit((_Atomic(double) *)&es->time.execute.sum, memory_order_relaxed);
            rcd.time_select    += atomic_load_explicit((_Atomic(double) *)&es->time.select.sum,  memory_order_relaxed);
#if defined(PARSEC_SIM_TIME)
            double sim_path = atomic_load_explicit((_Atomic(double) *)&es->largest_simulation_time, memory_order_relaxed);
            if ( rcd.sim_path < sim_path )
                rcd.sim_path = sim_path;
#endif /* PARSEC_SIM_TIME */
        }
    }

    parsec_graph_stat_print_record(outfile, pgs_thrd.bfmt, &rcd);
}

static inline void parsec_graph_stat_continue(void)
{
    int ret = 0;
    struct timespec timeout;
    /* use the clock model to determine how long the timeout is locally */
    long nsec = lround((double)pgs_thrd.waitns * parsec_stat_clock_model.skew);
    /* get current time */
    clock_gettime(CLOCK_MONOTONIC, &timeout);
    /* add timeout to current time */
    timespec_add_ns(&timeout, nsec);
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
    pgs_record_t zero_rcd = { .time_current    = 0.0,
                              .time_execution  = 0.0,
                              .time_select     = 0.0,
                              .time_wait       = 0.0,
                              .tasks_known     = 0,
                              .tasks_ready     = 0,
                              .tasks_executed  = 0,
                              .tasks_completed = 0,
                              .tasks_retired   = 0,
                              .threads_idle    = 0,
#if defined(PARSEC_SIM_TIME)
                              .sim_path        = 0.0,
#endif /* PARSEC_SIM_TIME */
    };
    /* we start with all threads idle */
    zero_rcd.threads_idle = vpmap_get_nb_total_threads();

    double time_start = 0.0;
    FILE *outfile = fopen(pgs_thrd.filename, (pgs_thrd.bfmt ? "wb" : "w"));

    parsec_graph_stat_print_header(outfile, pgs_thrd.bfmt);

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
            parsec_graph_stat_record(pgs_thrd.context, time_start, outfile,
                                     PGS_CONTINUE);
            prior_status = PGS_CONTINUE;
            /* wait for status change or timeout */
            parsec_graph_stat_continue();
            break;
        case PGS_PAUSE:
            /* record stats for last epoch, if necessary */
            parsec_graph_stat_record(pgs_thrd.context, time_start, outfile,
                                     prior_status);
            prior_status = PGS_PAUSE;
            /* reset counters
             * other threads might increment these before we get to reset them,
             * so do this here at end of prior epoch
             * instead of in PGS_RESET at beginning of next epoch */
            atomic_store_explicit(&pgs_thrd.data.known,     0, memory_order_release);
            atomic_store_explicit(&pgs_thrd.data.ready,     0, memory_order_release);
            atomic_store_explicit(&pgs_thrd.data.executed,  0, memory_order_release);
            atomic_store_explicit(&pgs_thrd.data.completed, 0, memory_order_release);
            atomic_store_explicit(&pgs_thrd.data.retired,   0, memory_order_release);
            atomic_store_explicit(&pgs_thrd.data.idle,      0, memory_order_release);
            /* wait until we're signaled */
            pthread_cond_wait(&pgs_thrd.cond, &pgs_thrd.mtx);
            break;
        case  PGS_STOP:
            /* record stats for last epoch, if necessary */
            parsec_graph_stat_record(pgs_thrd.context, time_start, outfile,
                                     prior_status);
            prior_status = PGS_STOP;
            /* stop the thread */
            stop = true;
            break;
        case PGS_RESET:
            /* record stats for last epoch, if necessary */
            parsec_graph_stat_record(pgs_thrd.context, time_start, outfile,
                                     prior_status);
            prior_status = PGS_RESET;
            /* reset internal structures */
            time_start = parsec_stat_time(&parsec_stat_clock_model);
            /* record first (zero) measurement */
            parsec_graph_stat_print_record(outfile, pgs_thrd.bfmt, &zero_rcd);
            /* set status to PGS_CONTINUE to start measurement */
            pgs_thrd.status = PGS_CONTINUE;
            /* wait for status change or timeout */
            parsec_graph_stat_continue();
            break;
        }
    }
    pthread_mutex_unlock(&pgs_thrd.mtx);

    fclose(outfile);
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
    parsec_mca_param_reg_int_name("stat", "bfmt",
                                  "Stat graph file binary format (default TSV)",
                                  false, false,
                                  pgs_thrd.bfmt, &pgs_thrd.bfmt);

    /* only start thread if explicitly asked to do so */
    if (filename) {
        /* append rank to filename */
        asprintf(&pgs_thrd.filename,
                 (pgs_thrd.bfmt ? "%s-%d.bfmt" : "%s-%d.tsv"),
                 filename, context->my_rank);
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
        /* unlock mutex and let thread reset itself */
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
        /* unlock mutex and let thread pause itself */
        pthread_mutex_unlock(&pgs_thrd.mtx);
    }
}
