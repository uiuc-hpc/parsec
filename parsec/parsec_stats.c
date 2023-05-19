#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <inttypes.h>
#include <math.h>
#include <time.h>

#include "parsec/parsec_config.h"
#include "parsec/parsec_internal.h"
#include "parsec/execution_stream.h"
#include "parsec/parsec_comm_engine.h"
#include "parsec/parsec_remote_dep.h"
#include "parsec/parsec_stats.h"

#if   defined(PARSEC_HAVE_MPI)
#include <mpi.h>
#elif defined(PARSEC_HAVE_LCI)
#include <lci.h>
#endif   /* PARSEC_HAVE_MPI */

#if defined(PARSEC_STATS)
parsec_stat_clock_model_t parsec_stat_clock_model = PARSEC_STAT_CLOCK_MODEL_INITIALIZER;
void parsec_stat_clock_model_init(const parsec_context_t* context,
                                  parsec_stat_clock_model_t *clk)
{
#if defined(DISTRIBUTED)
    parsec_ce.sync(&parsec_ce);
#endif /* DISTRIBUTED */
    clk->init = parsec_stat_time_raw();
#if defined(PARSEC_STATS_COMM)
    parsec_comm_stat_clock_model_init(context, clk);
#endif /* PARSEC_STATS_COMM */
}
#endif /* PARSEC_STATS */

#if defined(PARSEC_STATS_SCHED)
void parsec_sched_stat_print(const parsec_context_t *context)
{
    const struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
    double time_execute = 0.0;
    double time_select  = 0.0;
    double time_wait    = 0.0;
    for (int32_t vpid = 0; vpid < context->nb_vp; vpid++) {
        parsec_vp_t *vp = context->virtual_processes[vpid];
        for (int32_t thid = 0; thid < vp->nb_cores; thid++) {
            parsec_execution_stream_t *es = vp->execution_streams[thid];
            time_execute += es->time.execute.sum;
            time_select  += es->time.select.sum;
            time_wait    += es->time.wait.sum;
        }
    }
    for (int i = 0; i < context->nb_nodes; i++) {
        fflush(stdout);
        if (i == context->my_rank) {
            fprintf(stdout, "[%d]:\texecute(%f)\tselect(%f)\twait(%f)\n",
                    context->my_rank, time_execute, time_select, time_wait);
            fflush(stdout);
        }
#if defined(DISTRIBUTED)
        parsec_ce.sync(&parsec_ce);
        nanosleep(&ts, NULL);
#endif /* DISTRIBUTED */
    }
}

void parsec_sched_stat_reset(const parsec_context_t *context)
{
    for (int32_t vpid = 0; vpid < context->nb_vp; vpid++) {
        parsec_vp_t *vp = context->virtual_processes[vpid];
        for (int32_t thid = 0; thid < vp->nb_cores; thid++) {
            parsec_execution_stream_t *es = vp->execution_streams[thid];
            /* reset stats */
            es->time.execute = KAHAN_SUM_INITIALIZER;
            es->time.select  = KAHAN_SUM_INITIALIZER;
            es->time.wait    = KAHAN_SUM_INITIALIZER;
        }
    }
}
#endif /* PARSEC_STATS_SCHED */

#if defined(PARSEC_STATS_TC)
void parsec_tc_stat_print(const parsec_taskpool_t *tp)
{
    const struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
    parsec_context_t *context = tp->context;
    const parsec_task_class_t *tc;
    kahan_sum_t time_execute;

    for (int i = 0; i < context->nb_nodes; i++) {
        fflush(stdout);
        if (i == context->my_rank) {
            fprintf(stdout, "[%d]:", context->my_rank);
            for (uint32_t j = 0; j < tp->nb_task_classes; j++) {
                tc = tp->task_classes_array[j];
                time_execute = atomic_load_explicit(&tc->time_execute,
                                                    memory_order_relaxed);
                fprintf(stdout, "\t%s(%f)", tc->name, time_execute.sum);
            }
            fprintf(stdout, "\n");
            fflush(stdout);
        }
#if defined(DISTRIBUTED)
        parsec_ce.sync(&parsec_ce);
        nanosleep(&ts, NULL);
#endif /* DISTRIBUTED */
    }
}
#endif /* PARSEC_STATS_TC */


#if defined(PARSEC_STATS_COMM)
parsec_comm_stat_t parsec_comm_send_stat = PARSEC_COMM_STAT_INITIALIZER; /* stats for activation send -> dep send done */
parsec_comm_stat_t parsec_comm_sdep_stat = PARSEC_COMM_STAT_INITIALIZER; /* stats for activation send -> all send done */
parsec_comm_stat_t parsec_comm_recv_stat = PARSEC_COMM_STAT_INITIALIZER; /* stats for activation recv -> dep recv done */
parsec_comm_stat_t parsec_comm_rdep_stat = PARSEC_COMM_STAT_INITIALIZER; /* stats for activation recv -> all recv done */
parsec_comm_stat_t parsec_comm_actv_stat = PARSEC_COMM_STAT_INITIALIZER; /* stats for activation send -> activation recv */
parsec_comm_stat_t parsec_comm_srcv_stat = PARSEC_COMM_STAT_INITIALIZER; /* stats for activation send -> dep recv done, inter-node */
parsec_comm_stat_t parsec_comm_srdp_stat = PARSEC_COMM_STAT_INITIALIZER; /* stats for activation send -> all recv done, inter-node */
parsec_comm_stat_t parsec_comm_root_stat = PARSEC_COMM_STAT_INITIALIZER; /* stats for root activ send -> dep recv done, inter-node */
parsec_comm_stat_t parsec_comm_rtdp_stat = PARSEC_COMM_STAT_INITIALIZER; /* stats for root activ send -> all recv done, inter-node */

parsec_comm_engine_stat_t parsec_comm_engine_send_stat = PARSEC_COMM_ENGINE_STAT_INITIALIZER; /* stats for comm engine send */
parsec_comm_engine_stat_t parsec_comm_engine_recv_stat = PARSEC_COMM_ENGINE_STAT_INITIALIZER; /* stats for comm egnine recv */

/* send a double to rank */
static inline void parsec_comm_stat_time_delay_send(double *time, int rank)
{
#if   defined(PARSEC_HAVE_MPI)
    MPI_Send(time, 1, MPI_DOUBLE, rank, 0, MPI_COMM_WORLD);
#elif defined(PARSEC_HAVE_LCI)
    extern LCI_endpoint_t timer_ep;
    LCI_mbuffer_t mbuf = { .address = time, .length = sizeof(double) };
    while (LCI_OK != LCI_sendm(timer_ep, mbuf, rank, 0))
        continue;
#endif   /* PARSEC_HAVE_MPI */
}

/* recv a double from rank */
static inline void parsec_comm_stat_time_delay_recv(double *time, int rank)
{
#if   defined(PARSEC_HAVE_MPI)
    MPI_Recv(time, 1, MPI_DOUBLE, rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
#elif defined(PARSEC_HAVE_LCI)
    extern LCI_endpoint_t timer_ep;
    extern LCI_comp_t timer_comp;
    LCI_mbuffer_t mbuf = { .address = time, .length = sizeof(double) };
    while (LCI_OK != LCI_recvm(timer_ep, mbuf, rank, 0, timer_comp, NULL))
        continue;
    LCI_sync_wait(timer_comp, NULL);
#endif   /* PARSEC_HAVE_MPI */
}

typedef struct {
    kahan_sum_t x;
    kahan_sum_t y;
    kahan_sum_t xx;
    kahan_sum_t xy;
} linear_fit_t;

#if 0
typedef struct {
    double n;
    double x;
    double x2;
    double x3;
    double x4;
    double y;
    double xy;
    double x2y;
} quadratic_fit_t;
#endif

/* adapted from S. Hunold and A. Carpen-Amarie,
 * "Hierarchical Clock Synchronization in MPI", CLUSTER 2018
 * Algorithm 7 in Appendix A, SKaMPI-Offset */
#if   defined(PARSEC_HAVE_MPI)
#define TIME_DELAY_NEXCHANGES 40
#elif defined(PARSEC_HAVE_LCI)
#define TIME_DELAY_NEXCHANGES 200
#endif   /* PARSEC_HAVE_MPI */
fit_point_t parsec_comm_stat_time_delay(const parsec_context_t* context,
                                        const parsec_stat_clock_model_t *clk)
{
    double t_last, s_last, s_now;
    double td_max = +INFINITY;
    double td_min = -INFINITY;
    double td_max_t, td_min_t;
#if 0
    double duration_min = +INFINITY;
#endif
    fit_point_t fit_point = { .x = 0.0, .y = 0.0 };

    for (int rank = 1; rank < context->nb_nodes; rank++) {
        parsec_ce.sync(&parsec_ce);
        if (context->my_rank == 0) {
            for (size_t i = 0; i < TIME_DELAY_NEXCHANGES; i++) {
                parsec_comm_stat_time_delay_recv(&s_last, rank);
                t_last = parsec_stat_time(clk);
                parsec_comm_stat_time_delay_send(&t_last, rank);
            }
            fit_point.x = t_last;
            fit_point.y = t_last;
        } else if (context->my_rank == rank) {
            for (size_t i = 0; i < TIME_DELAY_NEXCHANGES; i++) {
                s_last = parsec_stat_time(clk);
                parsec_comm_stat_time_delay_send(&s_last, 0);
                parsec_comm_stat_time_delay_recv(&t_last, 0);
                s_now = parsec_stat_time(clk);
#if 0
                if ((s_now - s_last) < duration_min) {
                    duration_min = s_now - s_last;
                    fit_point.x = (s_now + s_last) / 2;
                    fit_point.y = t_last;
                }
#endif
                if ((t_last - s_now) > td_min) {
                    td_min = t_last - s_now;
                    td_min_t = s_now;
                }
                if ((t_last - s_last) < td_max) {
                    td_max = t_last - s_last;
                    td_max_t = s_last;
                }
            }
#if 1
            fit_point.x = (td_min_t + td_max_t) / 2;
            fit_point.y = fit_point.x + (td_min + td_max) / 2;
#endif
        }
    }
    return fit_point;
}

#define CLOCK_MODEL_NFITPOINTS 50
void parsec_comm_stat_clock_model_init(const parsec_context_t* context,
                                       parsec_stat_clock_model_t *clk)
{
    linear_fit_t fit = {
        .x  = KAHAN_SUM_INITIALIZER,
        .y  = KAHAN_SUM_INITIALIZER,
        .xx = KAHAN_SUM_INITIALIZER,
        .xy = KAHAN_SUM_INITIALIZER,
    };
    double n, x, y, xx, xy, denominator;
#if 0
    quadratic_fit_t fit = { .n = CLOCK_MODEL_NFITPOINTS,
                            .x = 0.0, .x2 = 0.0, .x3 = 0.0, .x4 = 0.0,
                            .y = 0.0, .xy = 0.0, .x2y = 0.0 };
#endif
    fit_point_t fit_point;

    if (context->nb_nodes == 1) {
        /* we're alone, no need to make a clock model */
        return;
    }

    for (size_t i = 0; i < CLOCK_MODEL_NFITPOINTS; i++) {
        fit_point = parsec_comm_stat_time_delay(context, clk);
        kahan_sum(&fit.x, fit_point.x);
        kahan_sum(&fit.y, fit_point.y);
        kahan_sum(&fit.xx, (fit_point.x * fit_point.x));
        kahan_sum(&fit.xy, (fit_point.x * fit_point.y));
#if 0
        fit.x  += fit_point.x;
        fit.x2  = fma(fit_point.x, fit_point.x, fit.x2);
        fit.x3  = fma(fit_point.x, (fit_point.x * fit_point.x), fit.x3);
        fit.x4  = fma((fit_point.x * fit_point.x), (fit_point.x * fit_point.x), fit.x4);
        fit.y  += fit_point.y;
        fit.xy  = fma(fit_point.x, fit_point.y, fit.xy);
        fit.x2y = fma((fit_point.x * fit_point.x), fit_point.y, fit.x2y);
#endif
    }

    n  = CLOCK_MODEL_NFITPOINTS;
    x  = fit.x.sum;
    y  = fit.y.sum;
    xx = fit.xx.sum;
    xy = fit.xy.sum;

    /* delay is accumulated additively
     * skew is accumulated multiplicatively */
    denominator = n * xx - x * x;
    clk->delay += (y * xx - x * xy) / denominator;
    clk->skew  *= (n * xy - x * y)  / denominator;
#if 0
    /* this is pretty ugly, but I don't think it can really be simplified
     * was determined symbolically with the help of WolframAlpha */
    denominator = fit.x*fit.x*fit.x4 - 2.0*fit.x*fit.x2*fit.x3 + fit.x2*fit.x2*fit.x2 - fit.x2*fit.x4*fit.n + fit.x3*fit.x3*fit.n;
    clk->delay  = (-fit.x*fit.x3*fit.x2y + fit.x*fit.x4*fit.xy + fit.x2*fit.x2*fit.x2y - fit.x2*fit.x3*fit.xy - fit.x2*fit.x4*fit.y  + fit.x3*fit.x3*fit.y ) / denominator;
    clk->skew   = (-fit.x*fit.x2*fit.x2y + fit.x*fit.x4*fit.y  + fit.x2*fit.x2*fit.xy  - fit.x2*fit.x3*fit.y  - fit.n*fit.x4*fit.xy  + fit.n*fit.x3*fit.x2y) / denominator;
    clk->curve  = (-fit.x*fit.x2*fit.xy  + fit.x*fit.x*fit.x2y + fit.x2*fit.x2*fit.y   - fit.x*fit.x3*fit.y   - fit.n*fit.x2*fit.x2y + fit.n*fit.x3*fit.xy ) / denominator;
#endif
    printf("%d:\tf(x) = %7.1f + %.10f*x\n", context->my_rank, clk->delay, clk->skew);
    fflush(stdout);

    parsec_ce.sync(&parsec_ce);
}

#define COMM_STAT_FMT(name) "\t" name "(%6zu, %7.3f)\t[%7.1f µs < %6.3f ms < %7.3f ms]\n"
#define COMM_STAT_EXPAND(stat)                        \
  (stat).count,                                       \
  (stat).lat.sum.sum * 1e-9,                          \
  (stat).lat.min * 1e-3,                              \
  (stat).lat.sum.sum / ((double)(stat).count) * 1e-6, \
  (stat).lat.max * 1e-6

#define COMM_ENGINE_STAT_FMT(name, f1, f2)                                         \
  "\t" name "(%6zu, %11zu B, %7.3f, %7.3f)\t[%3.0f ns < %7.2f µs < %7.3f ms]\n"    \
  "\t\t\t\t\t\t"                "\t[%7.2f " f1 " < %5.2f Gbit/s < %5.2f Gbit/s]\n" \
  "\t\t\t\t\t\t"                "\t[%7.2f " f2 " , %5.2f Gbit/s]\n"
#define COMM_ENGINE_STAT_EXPAND(stat, ovl, f1, f2)    \
  (stat).count,                                       \
  (stat).bytes,                                       \
  (stat).lat.sum.sum * 1e-9,                          \
  (ovl).sum * 1e-9,                                   \
  (stat).lat.min,                                     \
  (stat).lat.sum.sum / ((double)(stat).count) * 1e-3, \
  (stat).lat.max * 1e-6,                              \
  (stat).bw.min * (f1),                               \
  ((double)(stat).bytes) / (ovl).sum * 8,             \
  (stat).bw.max * 8e-9,                               \
  (stat).count / (stat).bw.inv_sum.sum * (f2),        \
  (stat).bw.sum.sum / (stat).count * 8e-9

static inline
void parsec_comm_stat_print_helper(FILE *stream, int rank, const fit_point_t *delay)
{
    kahan_sum_t sstat_ovl, rstat_ovl;
    sstat_ovl = atomic_load_explicit(&parsec_comm_engine_send_stat.lat.ovl,
                                     memory_order_relaxed);
    rstat_ovl = atomic_load_explicit(&parsec_comm_engine_recv_stat.lat.ovl,
                                     memory_order_relaxed);
    fprintf(stream,
            "[%d]:\t%f\n"
                  COMM_STAT_FMT(" send")
                  COMM_STAT_FMT(" sdep")
                  COMM_STAT_FMT(" recv")
                  COMM_STAT_FMT(" rdep")
                  COMM_STAT_FMT("*actv")
                  COMM_STAT_FMT("*srcv")
                  COMM_STAT_FMT("*srdp")
                  COMM_STAT_FMT("*root")
                  COMM_STAT_FMT("*rtdp")
                  COMM_ENGINE_STAT_FMT("^send", "Mbit/s", "Mbit/s")
                  COMM_ENGINE_STAT_FMT("^recv", " bit/s", "Kbit/s"),
            rank, (delay->x - delay->y),
            COMM_STAT_EXPAND(parsec_comm_send_stat),
            COMM_STAT_EXPAND(parsec_comm_sdep_stat),
            COMM_STAT_EXPAND(parsec_comm_recv_stat),
            COMM_STAT_EXPAND(parsec_comm_rdep_stat),
            COMM_STAT_EXPAND(parsec_comm_actv_stat),
            COMM_STAT_EXPAND(parsec_comm_srcv_stat),
            COMM_STAT_EXPAND(parsec_comm_srdp_stat),
            COMM_STAT_EXPAND(parsec_comm_root_stat),
            COMM_STAT_EXPAND(parsec_comm_rtdp_stat),
            COMM_ENGINE_STAT_EXPAND(parsec_comm_engine_send_stat, sstat_ovl, 8e-6, 8e-6),
            COMM_ENGINE_STAT_EXPAND(parsec_comm_engine_recv_stat, rstat_ovl, 8   , 8e-3));
}

#undef COMM_STAT_FMT
#undef COMM_STAT_EXPAND
#undef COMM_ENGINE_STAT_FMT
#undef COMM_ENGINE_STAT_EXPAND

void parsec_comm_stat_print(const parsec_context_t *context)
{
    const struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
    /* print stats for this epoch */
    fit_point_t fit_point = parsec_comm_stat_time_delay(context, &parsec_stat_clock_model);
    for (int i = 0; i < context->nb_nodes; i++) {
        fflush(stdout);
        if (i == context->my_rank) {
            parsec_comm_stat_print_helper(stdout, i, &fit_point);
            fflush(stdout);
        }
        parsec_ce.sync(&parsec_ce);
        nanosleep(&ts, NULL);
    }
}

void parsec_comm_stat_reset(const parsec_context_t *context)
{
    /* reset stats */
    parsec_comm_send_stat = PARSEC_COMM_STAT_INITIALIZER;
    parsec_comm_sdep_stat = PARSEC_COMM_STAT_INITIALIZER;
    parsec_comm_recv_stat = PARSEC_COMM_STAT_INITIALIZER;
    parsec_comm_rdep_stat = PARSEC_COMM_STAT_INITIALIZER;
    parsec_comm_actv_stat = PARSEC_COMM_STAT_INITIALIZER;
    parsec_comm_srcv_stat = PARSEC_COMM_STAT_INITIALIZER;
    parsec_comm_srdp_stat = PARSEC_COMM_STAT_INITIALIZER;
    parsec_comm_root_stat = PARSEC_COMM_STAT_INITIALIZER;
    parsec_comm_rtdp_stat = PARSEC_COMM_STAT_INITIALIZER;
    parsec_comm_engine_send_stat = PARSEC_COMM_ENGINE_STAT_INITIALIZER;
    parsec_comm_engine_recv_stat = PARSEC_COMM_ENGINE_STAT_INITIALIZER;
}

#endif /* PARSEC_STATS_COMM */
