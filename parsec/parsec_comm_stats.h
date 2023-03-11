#ifndef __PARSEC_COMM_STATS_H__
#define __PARSEC_COMM_STATS_H__

#define PARSEC_COMM_STATS
#if defined(PARSEC_COMM_STATS)

#include <stdatomic.h>
#include <stdalign.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <math.h>
#ifndef NSEC_PER_SEC
#define NSEC_PER_SEC (1000000000LL)
#endif


#include "parsec/parsec_config.h"
#include "parsec/execution_stream.h"
#include "parsec/parsec_comm_engine.h"
#include "parsec/parsec_remote_dep.h"

#if   defined(PARSEC_HAVE_MPI)
#include <mpi.h>
#elif defined(PARSEC_HAVE_LCI)
#include <lc.h>
#endif   /* PARSEC_HAVE_MPI */

typedef struct kahan_sum_s {
    double sum;
    double c;
} kahan_sum_t;

static inline void kahan_sum(kahan_sum_t *ksum, double value)
{
    double y = value - ksum->c;
    double t = ksum->sum + y;
    ksum->c = (t - ksum->sum) - y;
    ksum->sum = t;
}

#define KAHAN_SUM_INITIALIZER (const kahan_sum_t){ .sum = 0.0, .c = 0.0, }

typedef struct parsec_comm_stat_s {
    size_t count;
    struct {
        kahan_sum_t sum;
        double min;
        double max;
    } lat;
} parsec_comm_stat_t;

typedef struct {
    alignas(16) struct {
        double last;
        int64_t count;
    };
} comm_active_t;

typedef struct parsec_comm_engine_stat_s {
    size_t count;
    size_t bytes;
    _Atomic(comm_active_t) active;
    struct {
        kahan_sum_t sum;
        double min;
        double max;
        _Atomic(kahan_sum_t) ovl; /* sum, excluding overlap */
    } lat;
    struct {
        kahan_sum_t sum;
        kahan_sum_t inv_sum;
        double min;
        double max;
    } bw;
} parsec_comm_engine_stat_t;

#define TIME_T_MAX _Generic((time_t)0,                         \
                           char:                     CHAR_MAX, \
                           signed char:             SCHAR_MAX, \
                           unsigned char:           UCHAR_MAX, \
                           short int:                SHRT_MAX, \
                           unsigned short int:      USHRT_MAX, \
                           int:                       INT_MAX, \
                           unsigned int:             UINT_MAX, \
                           long int:                 LONG_MAX, \
                           unsigned long int:       ULONG_MAX, \
                           long long int:           LLONG_MAX, \
                           unsigned long long int: ULLONG_MAX)

#define PARSEC_COMM_STAT_INITIALIZER {        \
    .count = 0,                               \
    .lat = {                                  \
        .sum = KAHAN_SUM_INITIALIZER,         \
        .min = +INFINITY,                     \
        .max = -INFINITY,                     \
    },                                        \
  }

#define PARSEC_COMM_ENGINE_STAT_INITIALIZER { \
    .count = 0,                               \
    .bytes = 0,                               \
    .active = (comm_active_t){                \
        .last  = 0.0,                         \
        .count = 0,                           \
    },                                        \
    .lat = {                                  \
        .sum = KAHAN_SUM_INITIALIZER,         \
        .min = +INFINITY,                     \
        .max = -INFINITY,                     \
        .ovl = KAHAN_SUM_INITIALIZER,         \
    },                                        \
    .bw = {                                   \
        .sum     = KAHAN_SUM_INITIALIZER,     \
        .inv_sum = KAHAN_SUM_INITIALIZER,     \
        .min     = +INFINITY,                 \
        .max     = -INFINITY,                 \
    },                                        \
  }

/* returns time in ns */
static inline int64_t timespec_to_int64(struct timespec time)
{
    return ((int64_t)time.tv_sec * NSEC_PER_SEC) + (int64_t)time.tv_nsec;
}

/* returns time in ns */
static inline double timespec_to_double(struct timespec time)
{
    return (double)timespec_to_int64(time);
}

static inline double parsec_comm_stat_time(void)
{
    /* use CLOCK_MONOTONIC_RAW to get monotonic time without NTP or adjtime */
    struct timespec current_time;
    clock_gettime(CLOCK_MONOTONIC_RAW, &current_time);
    return timespec_to_double(current_time);
}

typedef struct parsec_comm_clock_model_s {
    double init;  /* local initial time */
    double delay; /* initial delay relative to reference clock; intercept */
    double skew;  /* skew relative to reference clock per ns; slope */
} parsec_comm_clock_model_t;

#define PARSEC_COMM_CLOCK_MODEL_INITIALIZER { \
    .init  = 0.0,                             \
    .delay = 0.0,                             \
    .skew  = 1.0,                             \
  }

extern parsec_comm_clock_model_t parsec_comm_clock_model;

static inline double parsec_comm_stat_time_since_init_local(void)
{
    return parsec_comm_stat_time() - parsec_comm_clock_model.init;
}

static inline double parsec_comm_stat_time_since_init(void)
{
    double local = parsec_comm_stat_time_since_init_local();
    return fma(local, parsec_comm_clock_model.skew, parsec_comm_clock_model.delay);
#if 0
    return fma(parsec_comm_clock_model.curve, local*local /* ax^2 */,
               fma(parsec_comm_clock_model.skew, local    /* bx   */,
                   parsec_comm_clock_model.delay          /* c    */));
#endif
}

/* send a double to rank */
static inline void parsec_comm_stat_time_delay_send(double *time, int rank,
                                                    parsec_context_t* context)
{
#if   defined(PARSEC_HAVE_MPI)
    MPI_Send(time, 1, MPI_DOUBLE, rank, 0, MPI_COMM_WORLD);
#elif defined(PARSEC_HAVE_LCI)
    while (LC_OK != lc_sends(time, sizeof(double), rank, 0,
                             *(lc_ep*)context->comm_ctx))
        continue;
#endif   /* PARSEC_HAVE_MPI */
}

/* recv a double from rank */
static inline void parsec_comm_stat_time_delay_recv(double *time, int rank,
                                                    parsec_context_t* context)
{
#if   defined(PARSEC_HAVE_MPI)
    MPI_Recv(time, 1, MPI_DOUBLE, rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
#elif defined(PARSEC_HAVE_LCI)
    lc_req req;
    while (LC_OK != lc_recvs(time, sizeof(double), rank, 0,
                             *(lc_ep*)context->comm_ctx, &req))
        continue;
    while (!req.sync)
        continue;
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

typedef struct {
    double x;
    double y;
} fit_point_t;

/* adapted from S. Hunold and A. Carpen-Amarie,
 * "Hierarchical Clock Synchronization in MPI", CLUSTER 2018
 * Algorithm 7 in Appendix A, SKaMPI-Offset */
#if   defined(PARSEC_HAVE_MPI)
#define TIME_DELAY_NEXCHANGES 40
#elif defined(PARSEC_HAVE_LCI)
#define TIME_DELAY_NEXCHANGES 200
#endif   /* PARSEC_HAVE_MPI */
static inline fit_point_t parsec_comm_stat_time_delay(parsec_context_t* context)
{
    double t_last, s_last, s_now;
    double td_max = +INFINITY;
    double td_min = -INFINITY;
    double td_max_t, td_min_t;
#if 0
    double duration_min = +INFINITY;
#endif
    fit_point_t fit_point = { .x = 0, .y = 0 };

    for (int rank = 1; rank < context->nb_nodes; rank++) {
        parsec_ce.sync(&parsec_ce);
        if (context->my_rank == 0) {
            for (size_t i = 0; i < TIME_DELAY_NEXCHANGES; i++) {
                parsec_comm_stat_time_delay_recv(&s_last, rank, context);
                t_last = parsec_comm_stat_time_since_init();
                parsec_comm_stat_time_delay_send(&t_last, rank, context);
            }
            fit_point.x = t_last;
            fit_point.y = t_last;
        } else if (context->my_rank == rank) {
            for (size_t i = 0; i < TIME_DELAY_NEXCHANGES; i++) {
                s_last = parsec_comm_stat_time_since_init();
                parsec_comm_stat_time_delay_send(&s_last, 0, context);
                parsec_comm_stat_time_delay_recv(&t_last, 0, context);
                s_now = parsec_comm_stat_time_since_init();
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
static inline void parsec_comm_stat_clock_model(parsec_context_t* context,
                                                parsec_comm_clock_model_t *clk)
{
    double delay_loop_start, current_time;
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

    parsec_ce.sync(&parsec_ce);
    clk->init = parsec_comm_stat_time();

    for (size_t i = 0; i < CLOCK_MODEL_NFITPOINTS; i++) {
        fit_point = parsec_comm_stat_time_delay(context);
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

    denominator = n * xx - x * x;
    clk->delay  = (y * xx - x * xy) / denominator;
    clk->skew   = (n * xy - x * y)  / denominator;
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

extern parsec_comm_stat_t parsec_comm_send_stat; /* stats for activation send -> dep send done */
extern parsec_comm_stat_t parsec_comm_sdep_stat; /* stats for activation send -> all send done */
extern parsec_comm_stat_t parsec_comm_recv_stat; /* stats for activation recv -> dep recv done */
extern parsec_comm_stat_t parsec_comm_rdep_stat; /* stats for activation recv -> all recv done */
extern parsec_comm_stat_t parsec_comm_actv_stat; /* stats for activation send -> activation recv */
extern parsec_comm_stat_t parsec_comm_srcv_stat; /* stats for activation send -> dep recv done, inter-node */
extern parsec_comm_stat_t parsec_comm_srdp_stat; /* stats for activation send -> all recv done, inter-node */
extern parsec_comm_stat_t parsec_comm_root_stat; /* stats for root activ send -> dep recv done, inter-node */
extern parsec_comm_stat_t parsec_comm_rtdp_stat; /* stats for root activ send -> all recv done, inter-node */

static inline
void parsec_comm_stat_update(parsec_comm_stat_t *stat, double duration)
{
    stat->count++;
    kahan_sum(&stat->lat.sum, duration);
    if (duration < stat->lat.min)
        stat->lat.min = duration;
    if (duration > stat->lat.max)
        stat->lat.max = duration;
}

static inline
void parsec_comm_engine_stat_update_last(parsec_comm_engine_stat_t *stat,
                                         double *time, int64_t increment)
{
    comm_active_t active_now, active_prior;
    double now, prior, duration;
    int64_t count;
    kahan_sum_t ovl_prior, ovl_now;

    active_prior = atomic_load_explicit(&stat->active, memory_order_acquire);
    do {
        now   = parsec_comm_stat_time_since_init();
        prior = active_prior.last;
        count = active_prior.count;

        active_now.last  = now;
        active_now.count = count + increment;

    } while (!atomic_compare_exchange_weak_explicit(&stat->active,
                                                    &active_prior, active_now,
                                  memory_order_acq_rel, memory_order_acquire));

    if (count > 0) {
        /* count > 0 means that comms were active between (prior, now) */
        duration = now - prior;
        ovl_prior = atomic_load_explicit(&stat->lat.ovl, memory_order_relaxed);
        do {
            ovl_now = ovl_prior;
            kahan_sum(&ovl_now, duration);
        } while (!atomic_compare_exchange_weak_explicit(&stat->lat.ovl,
                                                        &ovl_prior, ovl_now,
                                  memory_order_relaxed, memory_order_relaxed));
#if 0
        atomic_fetch_add_explicit(&stat->lat.ovl, duration,
                                  memory_order_relaxed);
#endif
    }

    *time = now;
}

static inline
void parsec_comm_engine_stat_update(parsec_comm_engine_stat_t *stat,
                                    size_t bytes, double duration)
{
    double duration_s = duration * 1e-9;
    double bw = (double)bytes / duration_s;
    double inv_bw = duration_s / (double)bytes;

    stat->count++;
    stat->bytes += bytes;
    kahan_sum(&stat->lat.sum, duration);
    if (duration < stat->lat.min)
        stat->lat.min = duration;
    if (duration > stat->lat.max)
        stat->lat.max = duration;
    kahan_sum(&stat->bw.sum, bw);
    kahan_sum(&stat->bw.inv_sum, inv_bw);
    if (bw < stat->bw.min)
        stat->bw.min = bw;
    if (bw > stat->bw.max)
        stat->bw.max = bw;
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
void parsec_comm_stat_print(FILE *stream, int rank, fit_point_t *delay,
                            const parsec_comm_engine_stat_t *sstat,
                            const parsec_comm_engine_stat_t *rstat)
{
    kahan_sum_t sstat_ovl, rstat_ovl;
    sstat_ovl = atomic_load_explicit(&sstat->lat.ovl, memory_order_relaxed);
    rstat_ovl = atomic_load_explicit(&rstat->lat.ovl, memory_order_relaxed);
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
            COMM_ENGINE_STAT_EXPAND(*sstat, sstat_ovl, 8e-6, 8e-6),
            COMM_ENGINE_STAT_EXPAND(*rstat, rstat_ovl, 8   , 8e-3));
}

#undef COMM_STAT_FMT
#undef COMM_STAT_EXPAND
#undef COMM_ENGINE_STAT_FMT
#undef COMM_ENGINE_STAT_EXPAND

#endif /* PARSEC_COMM_STATS */
#endif /* __PARSEC_COMM_STATS_H__ */
