#ifndef __PARSEC_STATS_H__
#define __PARSEC_STATS_H__

#include <stdatomic.h>
#include <stdalign.h>
#include <stdint.h>
#include <inttypes.h>
#include <math.h>
#include <time.h>

#include "parsec/parsec_config.h"
#include "parsec/runtime.h"

#if defined(PARSEC_STATS)

#ifndef NSEC_PER_SEC
#define NSEC_PER_SEC (1000000000LL)
#endif

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

static inline double parsec_stat_time_raw(void)
{
    /* use CLOCK_MONOTONIC_RAW to get monotonic time without NTP or adjtime */
    struct timespec current_time;
    clock_gettime(CLOCK_MONOTONIC_RAW, &current_time);
    return timespec_to_double(current_time);
}

typedef struct parsec_stat_clock_model_s {
    double init;  /* local initial time */
    double delay; /* initial delay relative to reference clock; intercept */
    double skew;  /* skew relative to reference clock per ns; slope */
} parsec_stat_clock_model_t;

#define PARSEC_STAT_CLOCK_MODEL_INITIALIZER \
  (const parsec_stat_clock_model_t) {       \
    .init  = 0.0,                           \
    .delay = 0.0,                           \
    .skew  = 1.0,                           \
  }

extern parsec_stat_clock_model_t parsec_stat_clock_model;

static inline
double parsec_stat_time_local(const parsec_stat_clock_model_t *clk)
{
    return parsec_stat_time_raw() - clk->init;
}

static inline
double parsec_stat_time(const parsec_stat_clock_model_t *clk)
{
    double local = parsec_stat_time_local(clk);
    return fma(local, clk->skew, clk->delay);
#if 0
    return fma(parsec_stat_clock_model.curve, local*local /* ax^2 */,
               fma(parsec_stat_clock_model.skew, local    /* bx   */,
                   parsec_stat_clock_model.delay          /* c    */));
#endif
}

void parsec_stat_clock_model_init(const parsec_context_t* context,
                                  parsec_stat_clock_model_t *clk);

#if defined(PARSEC_STATS_SCHED)
void parsec_sched_stat_print(const parsec_context_t *context);
#endif /* PARSEC_STATS_SCHED */

#if defined(PARSEC_STATS_COMM)
void parsec_comm_stat_clock_model_init(const parsec_context_t* context,
                                       parsec_stat_clock_model_t *clk);

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

#define PARSEC_COMM_STAT_INITIALIZER  \
  (const parsec_comm_stat_t) {        \
    .count = 0,                       \
    .lat = {                          \
        .sum = KAHAN_SUM_INITIALIZER, \
        .min = +INFINITY,             \
        .max = -INFINITY,             \
    },                                \
  }

#define PARSEC_COMM_ACTIVE_INITIALIZER \
  (const comm_active_t) {              \
    .last = 0.0,                       \
    .count = 0,                        \
  }

#define PARSEC_COMM_ENGINE_STAT_INITIALIZER   \
  (const parsec_comm_engine_stat_t) {         \
    .count = 0,                               \
    .bytes = 0,                               \
    .active = PARSEC_COMM_ACTIVE_INITIALIZER, \
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

extern parsec_comm_stat_t parsec_comm_send_stat; /* stats for activation send -> dep send done */
extern parsec_comm_stat_t parsec_comm_sdep_stat; /* stats for activation send -> all send done */
extern parsec_comm_stat_t parsec_comm_recv_stat; /* stats for activation recv -> dep recv done */
extern parsec_comm_stat_t parsec_comm_rdep_stat; /* stats for activation recv -> all recv done */
extern parsec_comm_stat_t parsec_comm_actv_stat; /* stats for activation send -> activation recv */
extern parsec_comm_stat_t parsec_comm_srcv_stat; /* stats for activation send -> dep recv done, inter-node */
extern parsec_comm_stat_t parsec_comm_srdp_stat; /* stats for activation send -> all recv done, inter-node */
extern parsec_comm_stat_t parsec_comm_root_stat; /* stats for root activ send -> dep recv done, inter-node */
extern parsec_comm_stat_t parsec_comm_rtdp_stat; /* stats for root activ send -> all recv done, inter-node */

extern parsec_comm_engine_stat_t parsec_comm_engine_send_stat; /* stats for comm engine send */
extern parsec_comm_engine_stat_t parsec_comm_engine_recv_stat; /* stats for comm egnine recv */

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
void parsec_comm_engine_stat_update_active(parsec_comm_engine_stat_t *stat,
                                           double *time, int64_t increment,
                                           const parsec_stat_clock_model_t *clk)
{
    comm_active_t active_now, active_prior;
    double now, prior, duration;
    int64_t count;
    kahan_sum_t ovl_prior, ovl_now;

    active_prior = atomic_load_explicit(&stat->active, memory_order_acquire);
    do {
        now   = parsec_stat_time(clk);
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
void parsec_comm_engine_stat_reset_active(parsec_comm_engine_stat_t *stat)
{
    atomic_store_explicit(&stat->active, PARSEC_COMM_ACTIVE_INITIALIZER,
                          memory_order_release);
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

typedef struct {
    double x;
    double y;
} fit_point_t;

fit_point_t parsec_comm_stat_time_delay(const parsec_context_t* context,
                                        const parsec_stat_clock_model_t *clk);

void parsec_comm_stat_print(const parsec_context_t *context);

#endif /* PARSEC_STATS_COMM */
#endif /* PARSEC_STATS */
#endif /* __PARSEC_STATS_H__ */
