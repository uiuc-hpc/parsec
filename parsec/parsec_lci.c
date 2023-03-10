/*
 * Copyright (c) 2009-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

/* C standard library */
#include <assert.h>
#include <inttypes.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <time.h>

/* POSIX */
#include <pthread.h>
#include <sched.h>

/* External libraries */
#include <lc.h>
#include <lc/dequeue.h>

/* PaRSEC */
#include "parsec/runtime.h"
#include "parsec/bindthread.h"
#include "parsec/execution_stream.h"
#include "parsec/mempool.h"
#include "parsec/papi_sde.h"
#include "parsec/parsec_comm_engine.h"
#include "parsec/parsec_internal.h"
#include "parsec/parsec_lci.h"
#include "parsec/parsec_remote_dep.h"
#include "parsec/class/list_item.h"
#include "parsec/class/parsec_hash_table.h"
#include "parsec/class/parsec_object.h"
#include "parsec/utils/debug.h"
#include "parsec/utils/mca_param.h"

/* ------- LCI implementation below ------- */

#if defined(PARSEC_PROF_TRACE) && 0
#define PARSEC_PROF_TRACE_LCI
#endif

#define LCI_ENABLE_EAGER (true)

typedef unsigned char byte_t;

#define lci_ce_output_verbose_lvl(LVL, FMT, ...)                              \
  do {                                                                        \
    parsec_output_verbose(LVL, parsec_comm_output_stream,                     \
                         "LCI[%d]:\t" FMT,                                    \
                         ep_rank, ##__VA_ARGS__);                             \
  } while (0)

#if 1
#define LCI_CE_DEBUG_VERBOSE(FMT, ...)                                        \
  do {                                                                        \
    PARSEC_DEBUG_VERBOSE(20, parsec_comm_output_stream,                       \
                         "LCI[%d]:\t" FMT,                                    \
                         ep_rank, ##__VA_ARGS__);                             \
  } while (0)
#else
#define LCI_CE_DEBUG_VERBOSE(FMT, ...)                                        \
  do {                                                                        \
    parsec_debug_history_add("Mark LCI[%d]:\t" FMT "\n",                      \
                             ep_rank, ##__VA_ARGS__);                         \
  } while (0)
#endif

#define lci_ce_debug_verbose_lvl(LVL, FMT, ...)                               \
  do {                                                                        \
    parsec_debug_verbose(LVL, parsec_comm_output_stream,                      \
                         "LCI[%d]:\t" FMT,                                    \
                         ep_rank, ##__VA_ARGS__);                             \
  } while (0)

#define lci_ce_fatal(FMT, ...)                                                \
  do {                                                                        \
    parsec_fatal("LCI[%d]:\t" FMT, ep_rank, ##__VA_ARGS__);                   \
  } while (0)

#ifdef PARSEC_LCI_RETRY_HISTOGRAM
static struct {
    atomic_size_t zero;
    atomic_size_t one;
    atomic_size_t five;
    atomic_size_t ten;
    atomic_size_t twenty;
    atomic_size_t fifty;
    atomic_size_t one_hundred;
    atomic_size_t greater;
} lci_retry_histogram;
#define RETRY(lci_call)                                                       \
  do {                                                                        \
      size_t count = 0;                                                       \
      while (LC_OK != (lci_call))                                             \
          count++;                                                            \
      if      (count <= 0)   lci_retry_histogram.zero++;                      \
      else if (count <= 1)   lci_retry_histogram.one++;                       \
      else if (count <= 5)   lci_retry_histogram.five++;                      \
      else if (count <= 10)  lci_retry_histogram.ten++;                       \
      else if (count <= 20)  lci_retry_histogram.twenty++;                    \
      else if (count <= 50)  lci_retry_histogram.fifty++;                     \
      else if (count <= 100) lci_retry_histogram.one_hundred++;               \
      else                   lci_retry_histogram.greater++;                   \
  } while (0)
#else /* PARSEC_LCI_RETRY_HISTOGRAM */
#define NS_PER_S 1000000000L
#define RETRY(lci_call) do { } while (LC_OK != (lci_call))
#if 0
#define RETRY(lci_call)                                                       \
  do {                                                                        \
    struct timespec _ts = { .tv_sec = 0, .tv_nsec = 1 };                      \
    lc_status _status = LC_OK;                                                \
    for (long _ns = 0; _ns < NS_PER_S; _ns += _ts.tv_nsec) {                  \
        LCI_CE_DEBUG_VERBOSE(#lci_call);                                      \
        _status = (lci_call);                                                 \
        if (LC_OK == _status)                                                 \
            break;                                                            \
        nanosleep(&_ts, NULL);                                                \
        _ts.tv_nsec *= 2;                                                     \
    }                                                                         \
    if (LC_OK != _status) {                                                   \
        lci_ce_fatal("retry failed: %d", _status);                            \
    }                                                                         \
  } while (0)
#endif
#endif /* PARSEC_LCI_RETRY_HISTOGRAM */

/* LCI memory handle type
 * PaRSEC object, inherits from parsec_list_item_t
 */
#if 0
typedef struct lci_mem_reg_handle_s {
    union {
        parsec_list_item_t    list_item; /* for mempool */
        struct {
            byte_t            *mem;
            //byte_t            *packed; /* pack buffer for noncontigiguous dt */
            size_t            size;
            size_t            count;
            parsec_datatype_t datatype;
        };
    };
    parsec_thread_mempool_t *mempool_owner;
} lci_mem_reg_handle_t;
#endif
typedef struct lci_mem_reg_handle_data_s {
    byte_t                         *mem;
//  byte_t                         *packed; /* pack buffer for noncontigiguous dt */
    size_t                         size;
    size_t                         count;
    parsec_datatype_t              datatype;
} lci_mem_reg_handle_data_t;

typedef struct lci_mem_reg_handle_s {
    alignas(64) struct {
        union {
            parsec_list_item_t        list_item; /* for mempool */
            lci_mem_reg_handle_data_t reg;
        };
        parsec_thread_mempool_t *mempool_owner;
    };
} lci_mem_reg_handle_t;
static PARSEC_OBJ_CLASS_INSTANCE(lci_mem_reg_handle_t, parsec_list_item_t, NULL, NULL);

/* memory pool for memory handles */
static parsec_mempool_t lci_mem_reg_handle_mempool;

/* types of callbacks
 * ordered by priority, least to greatest */
typedef enum lci_cb_handle_type_e {
    LCI_GET_TARGET           /* low priority, send is done */,
    LCI_PUT_ORIGIN           /* low priority, send is done */,
    LCI_ACTIVE_MESSAGE       /* med priority, user might start send */,
    LCI_GET_TARGET_HANDSHAKE /* med priority, must start send */,
    LCI_PUT_TARGET_HANDSHAKE /* med priority, must start recv */,
    LCI_GET_ORIGIN           /* high priority, recv done so task can start */,
    LCI_PUT_TARGET           /* high priority, recv done so task can start */,
    LCI_ABORT                /* highest priorty, must exit ASAP */,
    LCI_CB_HANDLE_TYPE_MAX
} lci_cb_handle_type_t;

/* LCI callback hash table type - 256 bytes */
typedef struct lci_cb_handle_s {
    alignas(256) parsec_list_item_t       list_item; /* pool/queue: 40 bytes */
    alignas(8)   parsec_thread_mempool_t  *mempool_owner;        /*  8 bytes */
#ifdef PARSEC_PROF_TRACE_LCI
    alignas(8)   uint64_t                 event_id;    /* optional:  8 bytes */
#endif
    alignas(64)  struct /* callback arguments */ {
        union {
            struct /* onesided callback arguments - 32B */ {
                parsec_ce_mem_reg_handle_t lreg;   /* local mem handle  */
                ptrdiff_t                  ldispl; /* local mem displ   */
                parsec_ce_mem_reg_handle_t rreg;   /* remote mem handle */
                ptrdiff_t                  rdispl; /* remote mem displ  */
            };
            struct /* AM callback arguments - 24B */ {
                parsec_ce_tag_t      tag;  /* AM tag  */
                byte_t               *msg; /* message */
            };
        };
        byte_t               *data;        /* callback data */
        size_t               size;         /* message size  */
        int                  remote;       /* remote peer   */
        lci_cb_handle_type_t type;         /* callback type */ /* here for size/alignment */
    } args;                                                      /* 56 bytes */
    alignas(8) union /* callback function */ {
        parsec_ce_am_callback_t       am;
        parsec_ce_onesided_callback_t put_origin;
        parsec_ce_am_callback_t       put_target;
        parsec_ce_onesided_callback_t get_origin;
        parsec_ce_am_callback_t       get_target;
    } cb;                                                        /*  8 bytes */
    alignas(64) lc_req req;               /* LCI request, for recv: 64 bytes */
                                                  /* padding: up to 64 bytes */
} lci_cb_handle_t;
static PARSEC_OBJ_CLASS_INSTANCE(lci_cb_handle_t, parsec_list_item_t, NULL, NULL);
static_assert(sizeof(lci_cb_handle_t) == 256, "lci_cb_handle_t size incorrect");
#define CB_HANDLE_PRIORITY (offsetof(lci_cb_handle_t, args.type) - \
                            offsetof(lci_cb_handle_t, list_item))

/* memory pool for callbacks */
static parsec_mempool_t lci_cb_handle_mempool;

/* AM registration handle */
typedef struct lci_am_reg_handle_s {
    byte_t                  *data;
    parsec_ce_am_callback_t cb;
#ifdef PARSEC_LCI_CB_HASH_TABLE
    parsec_hash_table_item_t ht_item;
#endif
} lci_am_reg_handle_t;

#ifdef PARSEC_LCI_CB_HASH_TABLE
/* hash table for AM callbacks */
static parsec_hash_table_t lci_am_cb_hash_table;
static parsec_key_fn_t lci_am_key_fns = {
    .key_equal = parsec_hash_table_generic_64bits_key_equal,
    .key_print = parsec_hash_table_generic_64bits_key_print,
    .key_hash  = parsec_hash_table_generic_64bits_key_hash
};
#else /* PARSEC_LCI_CB_HASH_TABLE */
#define LCI_AM_CB_TAG_MAX 16L
static lci_am_reg_handle_t lci_am_cb_array[LCI_AM_CB_TAG_MAX];
#endif /* PARSEC_LCI_CB_HASH_TABLE */

/* LCI dynamic message (for pool) */
typedef struct lci_dynmsg_s {
    alignas(128) parsec_list_item_t      list_item;
    alignas(8)   parsec_thread_mempool_t *mempool_owner;
    alignas(64)  byte_t                  data[]; /* cache-line alignment */
} lci_dynmsg_t;
static PARSEC_OBJ_CLASS_INSTANCE(lci_dynmsg_t, parsec_list_item_t, NULL, NULL);

/* memory pool for dynamic messages */
static parsec_mempool_t lci_dynmsg_mempool;

#define LCI_USE_TICKET_LOCK 0
#if       LCI_USE_TICKET_LOCK
typedef struct ticket_lock_s {
    atomic_size_t ticket;
    atomic_size_t lock;
} ticket_lock_t;

typedef struct ticket_s {
    size_t ticket;
    bool   valid;
} ticket_t;

#define TICKET_LOCK_INIT { .ticket = 0, .lock = 0 }
#define TICKET_INIT      { .ticket = 0, .valid = false }

static inline void ticket_lock_acquire(ticket_lock_t *tlock, ticket_t *ticket)
{
    if (!ticket->valid) {
        ticket->ticket = atomic_fetch_add_explicit(&tlock->ticket, 1, memory_order_relaxed);
        ticket->valid  = true;
    }
}

static inline bool ticket_lock_check(const ticket_lock_t *tlock, ticket_t *ticket)
{
    return ticket->valid &&
           ticket->ticket == atomic_load_explicit(&tlock->lock, memory_order_acquire);
}

static inline void ticket_lock_release(ticket_lock_t *tlock, ticket_t *ticket)
{
    atomic_fetch_add_explicit(&tlock->lock, 1, memory_order_acq_rel);
    ticket->valid = false;
}

static inline void ticket_lock_clear(ticket_lock_t *tlock, ticket_t *ticket)
{
    if (ticket->valid) {
        while (!ticket_lock_check(tlock, ticket))
            continue;
        ticket_lock_release(tlock, ticket);
    }
}

static ticket_lock_t lci_shared_tlock = TICKET_LOCK_INIT;
static ticket_t lci_progress_ticket = TICKET_INIT;
static ticket_t lci_comm_ticket     = TICKET_INIT;
#endif /* LCI_USE_TICKET_LOCK*/
static parsec_list_t lci_shared_cb_fifo;
static parsec_list_t lci_progress_cb_fifo;
static parsec_list_t lci_comm_cb_fifo;

static parsec_list_t lci_am_fifo;
static parsec_list_t lci_dynamic_fifo;

#if       LCI_USE_TICKET_LOCK
#define LCI_FIFO_LOCK_INIT(  ticket) ticket_lock_acquire(&lci_shared_tlock, (ticket))
#define LCI_FIFO_LOCK_TRY(   ticket) ticket_lock_check(  &lci_shared_tlock, (ticket))
#define LCI_FIFO_LOCK_UNLOCK(ticket) ticket_lock_release(&lci_shared_tlock, (ticket))
#define LCI_FIFO_LOCK_CLEAR( ticket) ticket_lock_clear(  &lci_shared_tlock, (ticket))
#define LCI_FIFO_LOCK_PROGRESS (&lci_progress_ticket)
#define LCI_FIFO_LOCK_COMM     (&lci_comm_ticket)
#else  /* LCI_USE_TICKET_LOCK */
#define LCI_FIFO_LOCK_INIT(  ticket) do { } while (0)
#define LCI_FIFO_LOCK_TRY(   ticket) parsec_atomic_trylock(&lci_shared_cb_fifo.atomic_lock)
#define LCI_FIFO_LOCK_UNLOCK(ticket) parsec_atomic_unlock( &lci_shared_cb_fifo.atomic_lock)
#define LCI_FIFO_LOCK_CLEAR( ticket) do { } while (0)
#define LCI_FIFO_LOCK_PROGRESS (0)
#define LCI_FIFO_LOCK_COMM     (1)
#endif /* LCI_USE_TICKET_LOCK */

#ifdef PARSEC_PROF_TRACE_LCI
static atomic_size_t queue_len = 0;
#endif

static inline bool lci_fifo_try_chain(void)
{
    parsec_list_item_t *ring = NULL;
    bool succeed = false;
    LCI_FIFO_LOCK_INIT(LCI_FIFO_LOCK_PROGRESS);
    succeed = LCI_FIFO_LOCK_TRY(LCI_FIFO_LOCK_PROGRESS);
    if (succeed) {
        ring = parsec_list_nolock_unchain(&lci_progress_cb_fifo);
//      parsec_list_nolock_chain_sorted(&lci_shared_cb_fifo, ring,
//                                      CB_HANDLE_PRIORITY);
        parsec_list_nolock_chain_back(&lci_shared_cb_fifo, ring);
        LCI_FIFO_LOCK_UNLOCK(LCI_FIFO_LOCK_PROGRESS);
    }
    return succeed;
}

#define LCI_FIFO_SPIN_UNCHAIN 0
static inline parsec_list_item_t * lci_fifo_try_unchain(void)
{
    parsec_list_item_t *ring = NULL;
    LCI_FIFO_LOCK_INIT(LCI_FIFO_LOCK_COMM);
#if       LCI_FIFO_SPIN_UNCHAIN
    while (!LCI_FIFO_LOCK_TRY(LCI_FIFO_LOCK_COMM))
        continue;
    ring = parsec_list_nolock_unchain(&lci_shared_cb_fifo);
    LCI_FIFO_LOCK_UNLOCK(LCI_FIFO_LOCK_COMM);
#else  /* LCI_FIFO_SPIN_UNCHAIN */
    if (LCI_FIFO_LOCK_TRY(LCI_FIFO_LOCK_COMM)) {
        ring = parsec_list_nolock_unchain(&lci_shared_cb_fifo);
        LCI_FIFO_LOCK_UNLOCK(LCI_FIFO_LOCK_COMM);
    }
#endif /* LCI_FIFO_SPIN_UNCHAIN */
    return ring;
}

static inline void lci_fifo_push(lci_cb_handle_t *handle, parsec_list_t *list)
{
#ifdef PARSEC_PROF_TRACE_LCI
    atomic_fetch_add_explicit(&queue_len, 1, memory_order_relaxed);
#endif
#if 1
    while (!parsec_atomic_trylock(&list->atomic_lock))
        continue;
    parsec_list_nolock_push_back(list, &handle->list_item);
    parsec_atomic_unlock(&list->atomic_lock);
#else
    parsec_list_push_back(list, &handle->list_item);
#endif
}

static inline void lci_am_fifo_push(lci_cb_handle_t *handle)
{
//  parsec_list_nolock_push_sorted(&lci_progress_cb_fifo, &handle->list_item,
//                                 CB_HANDLE_PRIORITY);
//  parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
//  parsec_list_push_sorted(&lci_shared_cb_fifo, &handle->list_item,
//                          CB_HANDLE_PRIORITY);
    lci_fifo_push(handle, &lci_am_fifo);
//  parsec_list_push_back(&lci_shared_cb_fifo, &handle->list_item);
//  lci_fifo_try_chain();
}

static inline void lci_dynamic_fifo_push(lci_cb_handle_t *handle)
{
    lci_fifo_push(handle, &lci_dynamic_fifo);
}

/* LCI one-sided activate message handshake header type */
typedef struct lci_handshake_header_s {
    byte_t            *buffer;
    size_t            size;
    size_t            cb_size;
    parsec_ce_tag_t   cb;
} lci_handshake_header_t;

/* LCI one-sided activate message handshake type */
typedef struct lci_handshake_s {
    alignas(64) lci_handshake_header_t header;
    alignas(8) byte_t data[];
} lci_handshake_t;

/* max size of handshake buffer on stack */
#define HANDSHAKE_STACK_BUFFER_SIZE (128UL)

/* these typedefs are not in the current standard, for some reason */
typedef _Atomic(uint16_t) atomic_uint16_t;
typedef _Atomic(uint64_t) atomic_uint64_t;

static inline int lci_get_tag(void)
{
    static atomic_uint16_t tag = 0;
    return atomic_fetch_add_explicit(&tag, 1, memory_order_relaxed);
}

/* global endpoint */
lc_ep *lci_global_ep = NULL;
static lc_ep *default_ep = NULL;
/* abort endpoint */
static lc_ep abort_ep;
/* collective endpoint */
static lc_ep collective_ep;
/* endpoint for the generic active messages */
static lc_ep am_ep;
/* endpoint for the one-sided active messages */
static lc_ep put_am_ep;
static lc_ep get_am_ep;
/* endpoint for one-sided put/get */
static lc_ep put_ep;
static lc_ep get_ep;

/* process rank and endpoint size */
static int ep_rank = 0;
static int ep_size = 0;

#if 0
static struct {
    pthread_cond_t  cond;
    pthread_mutex_t mutex;
    enum { LCI_RUN, LCI_PAUSE, LCI_STOP } status;
} progress_continue = { PTHREAD_COND_INITIALIZER, PTHREAD_MUTEX_INITIALIZER, LCI_STOP };
#endif
static struct {
    pthread_cond_t  cond;
    pthread_mutex_t mutex;
    _Bool           status;
} progress_start = { PTHREAD_COND_INITIALIZER, PTHREAD_MUTEX_INITIALIZER, false };
static int lci_comm_yield = 1;
static int progress_thread_binding = -1;
static atomic_bool progress_thread_stop = false;
static pthread_t progress_thread_id;

static inline void * lci_dyn_alloc(size_t size, void **ctx)
{
    assert(size <= lc_max_medium(0) && "dynamic message data too long");
    /* if size is 0, return NULL; else allocate from mempool */
    if (size == 0) {
        return NULL;
    }

    lci_dynmsg_t *dynmsg = parsec_thread_mempool_allocate(
                                           lci_dynmsg_mempool.thread_mempools);
    return &dynmsg->data;
}

#ifdef PARSEC_LCI_MESSAGE_LIMIT
static size_t lci_max_message = SIZE_MAX;
static atomic_size_t lci_message_count = 0;

static inline void lci_message_count_inc(void)
{
    atomic_fetch_add_explicit(&lci_message_count, 1, memory_order_relaxed);
}

static inline void lci_message_count_dec(void)
{
    atomic_fetch_sub_explicit(&lci_message_count, 1, memory_order_relaxed);
}

static inline void lci_message_count_sub(size_t subtrahend)
{
    atomic_fetch_sub_explicit(&lci_message_count, subtrahend, memory_order_relaxed);
}

static inline size_t lci_message_count_load(void)
{
    return atomic_load_explicit(&lci_message_count, memory_order_relaxed);
}
#endif

#ifdef PARSEC_LCI_HANDLER_COUNT
static struct {
    atomic_size_t progress;
    atomic_size_t other;
} lci_handler_thread[LCI_CB_HANDLE_TYPE_MAX];
static inline void LCI_HANDLER_PROGRESS(lci_cb_handle_type_t type) { lci_handler_thread[type].progress++; }
static inline void LCI_HANDLER_OTHER(lci_cb_handle_type_t type) { lci_handler_thread[type].other++; }
atomic_size_t lci_call_count[3];
static inline void LCI_CALL_AM()  { lci_call_count[0]++; }
static inline void LCI_CALL_PUT() { lci_call_count[1]++; }
static inline void LCI_CALL_GET() { lci_call_count[2]++; }
#else /* PARSEC_LCI_HANDLER_COUNT */
#define LCI_HANDLER_PROGRESS(CB_HANDLE_TYPE)
#define LCI_HANDLER_OTHER(CB_HANDLE_TYPE)
#define LCI_CALL_AM()
#define LCI_CALL_PUT()
#define LCI_CALL_GET()
#endif /* PARSEC_LCI_HANDLER_COUNT */

#if defined(PARSEC_PROF_TRACE_LCI)
static parsec_profiling_stream_t *comm_prof;
static parsec_profiling_stream_t *prog_prof;
static int LCI_AM_RCV_sk,   LCI_AM_RCV_ek;
static int LCI_PUT_HS_sk,   LCI_PUT_HS_ek;
static int LCI_PUT_DATA_sk, LCI_PUT_DATA_ek;
static int LCI_PUT_CB_sk,   LCI_PUT_CB_ek;
static int LCI_CB_PROG_sk,  LCI_CB_PROG_ek;

typedef struct {
    uint64_t tag;
    uint64_t size;
    uint64_t cb_size;
    int src;
    int dst;
} lci_prof_info_t;
static const char *lci_prof_info_str = "tag{uint64_t};size{uint64_t};cb_size{uint64_t};src{int};dst{int}";

#define CB_SIZE_NULL (UINT64_MAX)

static inline uint64_t lci_profile_event_id(void)
{
    static atomic_uint64_t event_id = 0;
    return atomic_fetch_add_explicit(&event_id, 1, memory_order_relaxed);
}

static inline void
lci_profile_info(parsec_profiling_stream_t *prof, int key, uint64_t event_id,
            uint64_t tag, uint64_t size, uint64_t cb_size, int src, int dst)
{
    lci_prof_info_t info = { .tag = tag, .size = size, .cb_size = cb_size,
                             .src = src, .dst = dst };
    PARSEC_PROFILING_TRACE(prof, key, event_id, PROFILE_OBJECT_ID_NULL, &info);
#if 0
    PARSEC_PROFILING_TRACE(prof, key, event_id, PROFILE_OBJECT_ID_NULL,
                           (&(lci_prof_info_t){tag, size, cb_size, src, dst}));
#endif
}

static inline void
lci_profile(parsec_profiling_stream_t *prof, int key, uint64_t event_id)
{
    PARSEC_PROFILING_TRACE(prof, key, event_id, PROFILE_OBJECT_ID_NULL, NULL);
}

typedef struct {
    uint32_t queue_len;
    uint32_t iter;
} lci_prof_cb_prog_t;
static const char *lci_prof_cb_prog_str = "queue{uint32_t};iter{uint32_t}";

static inline void
lci_profiling_init(void)
{
    parsec_profiling_add_dictionary_keyword("LCI_AM_RCV", "fill:#FF0000",
                                            sizeof(lci_prof_info_t),
                                            lci_prof_info_str,
                                            &LCI_AM_RCV_sk, &LCI_AM_RCV_ek);
    parsec_profiling_add_dictionary_keyword("LCI_PUT_HS", "fill:#FF0000",
                                            sizeof(lci_prof_info_t),
                                            lci_prof_info_str,
                                            &LCI_PUT_HS_sk, &LCI_PUT_HS_ek);
    parsec_profiling_add_dictionary_keyword("LCI_PUT_DATA", "fill:#FF0000",
                                            sizeof(lci_prof_info_t),
                                            lci_prof_info_str,
                                            &LCI_PUT_DATA_sk, &LCI_PUT_DATA_ek);
    parsec_profiling_add_dictionary_keyword("LCI_PUT_CB", "fill:#FF0000",
                                            sizeof(lci_prof_info_t),
                                            lci_prof_info_str,
                                            &LCI_PUT_CB_sk, &LCI_PUT_CB_ek);
    parsec_profiling_add_dictionary_keyword("LCI_CB_PROG", "fill:#FF0000",
                                            sizeof(lci_prof_cb_prog_t),
                                            lci_prof_cb_prog_str,
                                            &LCI_CB_PROG_sk, &LCI_CB_PROG_ek);

    comm_prof = parsec_comm_es.es_profile;
    prog_prof = parsec_profiling_stream_init(2*1024*1024, "Progress thread");
}
#endif /* PARSEC_PROF_TRACE_LCI */



/******************************************************************************
 *            Wrappers for executing callbacks from their handles             *
 *****************************************************************************/

/* call the callback in handle and free all data */
static inline void
lci_active_message_callback(lci_cb_handle_t *handle, parsec_comm_engine_t *comm_engine)
{
    assert(handle->args.type == LCI_ACTIVE_MESSAGE && "wrong handle type");
    LCI_CE_DEBUG_VERBOSE("Active Message %"PRIu64" cb:\t%d -> %d message %p size %zu",
                         handle->args.tag, handle->args.remote, ep_rank,
                         (void *)handle->args.msg, handle->args.size);
    handle->cb.am(comm_engine, handle->args.tag,
                  handle->args.msg, handle->args.size,
                  handle->args.remote, handle->args.data);
    /* return memory to pool (only allocated if size > 0) */
    if (handle->args.size > 0) {
        lci_dynmsg_t *dynmsg = container_of(handle->args.msg, lci_dynmsg_t, data);
        parsec_mempool_free(&lci_dynmsg_mempool, dynmsg);
    }
    /* return handle to mempool */
    parsec_mempool_free(&lci_cb_handle_mempool, handle);
}

/* call the callback in handle and free all data */
static inline void
lci_put_origin_callback(lci_cb_handle_t *handle, parsec_comm_engine_t *comm_engine)
{
    assert(handle->args.type == LCI_PUT_ORIGIN && "wrong handle type");
    LCI_CE_DEBUG_VERBOSE("Put Origin cb:\t%d(%p+%td) -> %d(%p+%td) size %zu data %p",
                         ep_rank,
                         (void *)((lci_mem_reg_handle_data_t *)handle->args.lreg)->mem,
                         handle->args.ldispl,
                         handle->args.remote,
                         (void *)((lci_mem_reg_handle_data_t *)handle->args.rreg)->mem,
                         handle->args.rdispl,
                         handle->args.size, handle->args.data);
    handle->cb.put_origin(comm_engine,
                          handle->args.lreg, handle->args.ldispl,
                          handle->args.rreg, handle->args.rdispl,
                          handle->args.size, handle->args.remote,
                          handle->args.data);
    /* return handle to mempool */
    parsec_mempool_free(&lci_cb_handle_mempool, handle);
}

/* call the callback in handle and free all data */
static inline void
lci_put_target_callback(lci_cb_handle_t *handle, parsec_comm_engine_t *comm_engine)
{
    assert(handle->args.type == LCI_PUT_TARGET && "wrong handle type");
    LCI_CE_DEBUG_VERBOSE("Put Target cb:\t%d -> %d(%p) size %zu data %p with tag %d",
                         handle->args.remote, ep_rank,
                         (void *)handle->args.msg, handle->args.size,
                         (void *)handle->args.data, handle->args.tag);
#if 0
    handle->cb.put_target(comm_engine,         handle->args.tag,
                          handle->args.msg,    handle->args.size,
                          handle->args.remote, handle->args.data);
#endif
    /* API seems to be bugged, pass callback data as message */
    handle->cb.put_target(comm_engine,         handle->args.tag,
                          handle->args.data,   handle->args.size,
                          handle->args.remote, NULL);
    /* return memory from AM */
    /* handle->args.data points to the data field of the handshake info
     * which is the data field of the dynmsg object */
    lci_handshake_t *handshake = container_of(handle->args.data, lci_handshake_t, data);
    lci_dynmsg_t    *dynmsg    = container_of(handshake, lci_dynmsg_t, data);
    parsec_mempool_free(&lci_dynmsg_mempool, dynmsg);
    /* return handle to mempool */
    parsec_mempool_free(&lci_cb_handle_mempool, handle);
}

/* call the callback in handle and free all data */
static inline void
lci_get_origin_callback(lci_cb_handle_t *handle, parsec_comm_engine_t *comm_engine)
{
    assert(handle->args.type == LCI_GET_ORIGIN && "wrong handle type");
    LCI_CE_DEBUG_VERBOSE("Get Origin cb:\t%d(%p+%td) <- %d(%p+%td) size %zu data %p",
                         ep_rank,
                         (void *)((lci_mem_reg_handle_data_t *)handle->args.lreg)->mem,
                         handle->args.ldispl,
                         handle->args.remote,
                         (void *)((lci_mem_reg_handle_data_t *)handle->args.rreg)->mem,
                         handle->args.rdispl,
                         handle->args.size, handle->args.data);
    handle->cb.get_origin(comm_engine,
                          handle->args.lreg, handle->args.ldispl,
                          handle->args.rreg, handle->args.rdispl,
                          handle->args.size, handle->args.remote,
                          handle->args.data);
    /* return handle to mempool */
    parsec_mempool_free(&lci_cb_handle_mempool, handle);
}

/* call the callback in handle and free all data */
static inline void
lci_get_target_callback(lci_cb_handle_t *handle, parsec_comm_engine_t *comm_engine)
{
    assert(handle->args.type == LCI_GET_TARGET && "wrong handle type");
    LCI_CE_DEBUG_VERBOSE("Get Target cb:\t%d <- %d(%p) size %zu with tag %d",
                         handle->args.remote, ep_rank,
                         (void *)handle->args.msg, handle->args.size,
                         handle->args.tag);
#if 0
    handle->cb.get_target(comm_engine,         handle->args.tag,
                          handle->args.msg,    handle->args.size,
                          handle->args.remote, handle->args.data);
#endif
    /* API seems to be bugged, pass callback data as message */
    handle->cb.get_target(comm_engine,         handle->args.tag,
                          handle->args.data,   handle->args.size,
                          handle->args.remote, NULL);
    /* return memory from AM */
    /* handle->args.data points to the data field of the handshake info
     * which is the data field of the dynmsg object */
    lci_handshake_t *handshake = container_of(handle->args.data, lci_handshake_t, data);
    lci_dynmsg_t    *dynmsg    = container_of(handshake, lci_dynmsg_t, data);
    parsec_mempool_free(&lci_dynmsg_mempool, dynmsg);
    /* return handle to mempool */
    parsec_mempool_free(&lci_cb_handle_mempool, handle);
}



/******************************************************************************
 *                Handlers for LCI send and receive functions                 *
 *****************************************************************************/

/* handler for abort
 * only called from within lc_progress (i.e. on progress thread) */
static inline void lci_abort_handler(lc_req *req)
{
    lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
    handle->args.tag    = req->meta;
    handle->args.remote = req->rank;
    handle->args.type   = LCI_ABORT;
    LCI_CE_DEBUG_VERBOSE("Abort %d recv:\t%d -> %d",
                         (int)handle->args.tag, handle->args.remote, ep_rank);
    LCI_HANDLER_PROGRESS(LCI_ABORT);
#if 0
    /* push to front of local callback fifo */
    parsec_list_nolock_push_front(&lci_progress_cb_fifo, &handle->list_item);
#endif
    lci_dynamic_fifo_push(handle);
}

/* handler for active message
 * only called from within lc_progress (i.e. on progress thread) */
static inline void lci_active_message_handler(lc_req *req)
{
    lci_am_reg_handle_t *am_handle = NULL;
    parsec_ce_tag_t tag = req->meta;
    LCI_CE_DEBUG_VERBOSE("Active Message %"PRIu64" recv:\t%d -> %d message %p size %zu",
                         tag, req->rank, ep_rank, req->buffer, req->size);

#ifdef PARSEC_PROF_TRACE_LCI
    uint64_t event_id = lci_profile_event_id();
    lci_profile_info(prog_prof, LCI_AM_RCV_sk, event_id, req->meta,
                     req->size, CB_SIZE_NULL, req->rank, ep_rank);
#endif

    /* find AM handle, based on active message tag */
#ifdef PARSEC_LCI_CB_HASH_TABLE
    am_handle = parsec_hash_table_nolock_find(&lci_am_cb_hash_table, tag);
#else
    am_handle = &lci_am_cb_array[tag];
#endif /* PARSEC_LCI_CB_HASH_TABLE */

    /* warn if not found  - in critical path, only with debug */
#ifndef NDEBUG
    if ((NULL == am_handle) || (NULL == am_handle->cb)) {
        parsec_warning("LCI[%d]:\tActive Message %"PRIu64" not registered",
                       ep_rank, tag);
        return;
    }
#endif /* NDEBUG */

    /* allocate callback handle & fill with info */
    lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
    handle->args.tag    = req->meta;
    handle->args.msg    = req->buffer;
    handle->args.data   = am_handle->data;
    handle->args.size   = req->size;
    handle->args.remote = req->rank;
    handle->args.type   = LCI_ACTIVE_MESSAGE;
    handle->cb.am       = am_handle->cb;
#ifdef PARSEC_PROF_TRACE_LCI
    handle->event_id    = event_id;
#endif

    LCI_HANDLER_PROGRESS(LCI_ACTIVE_MESSAGE);
#if 0
    /* push to local callback fifo */
    parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
#endif
    lci_am_fifo_push(handle);
}

/* handler for put on origin
 * using rendezvous recv, should only be called on progress thread */
static inline void lci_put_origin_handler(void *ctx)
{
#if 0
#ifdef PARSEC_LCI_MESSAGE_LIMIT
    /* send is complete, decrement message count */
    lci_message_count_dec();
#endif
#endif
    /* ctx is pointer to lci_cb_handle_t */
    lci_cb_handle_t *handle = ctx;
    assert(handle->args.type == LCI_PUT_ORIGIN && "wrong handle type");
    LCI_CE_DEBUG_VERBOSE("Put Origin end:\t%d(%p+%td) -> %d(%p+%td) size %zu",
                         ep_rank,
                         (void *)((lci_mem_reg_handle_data_t *)handle->args.lreg)->mem,
                         handle->args.ldispl,
                         handle->args.remote,
                         (void *)((lci_mem_reg_handle_data_t *)handle->args.rreg)->mem,
                         handle->args.rdispl,
                         handle->args.size);
    /* send is always large, so this is always called on progress thread
     * push to back of progress cb fifo */
    LCI_HANDLER_PROGRESS(LCI_PUT_ORIGIN);
#if 0
    parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
#endif
    lci_dynamic_fifo_push(handle);
#if 0
    if (pthread_equal(progress_thread_id, pthread_self())) {
        LCI_HANDLER_PROGRESS(LCI_PUT_ORIGIN);
        /* send was large, so we are on progress thread
         * push to back of progress cb fifo */
        parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
    } else {
        LCI_HANDLER_OTHER(LCI_PUT_ORIGIN);
        /* send was small or medium, so we are on caller thread
         * assume this is communication thread for now
         * push to back of comm cb fifo */
        parsec_list_nolock_push_back(&lci_comm_cb_fifo,  &handle->list_item);
    }
#endif
}

/* handler for put handshake on target
 * only called from within lc_progress (i.e. on progress thread) */
static inline void lci_put_target_handshake_handler(lc_req *req)
{
    lci_handshake_t *handshake = req->buffer;

#ifdef PARSEC_PROF_TRACE_LCI
    uint64_t event_id = lci_profile_event_id();
    lci_profile_info(prog_prof, LCI_PUT_HS_sk, event_id, req->meta,
                     handshake->header.size, handshake->header.cb_size,
                     req->rank, ep_rank);
#endif

    size_t buffer_size = sizeof(lci_handshake_t) + handshake->header.cb_size;
    size_t buffer_size_eager = buffer_size + handshake->header.size;
    bool send_eager = LCI_ENABLE_EAGER && (buffer_size_eager <= lc_max_medium(0));

    if (send_eager) {
        buffer_size = buffer_size_eager;
    }

    lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
    handle->args.tag         = req->meta;
    handle->args.msg         = handshake->header.buffer;
    /* array to pointer decay for handshake->data */
    handle->args.data        = handshake->data;
    handle->args.size        = handshake->header.size;
    handle->args.remote      = req->rank;
    handle->args.type        = LCI_PUT_TARGET;
    handle->cb.put_target    = (parsec_ce_am_callback_t)handshake->header.cb;
#ifdef PARSEC_PROF_TRACE_LCI
    handle->event_id         = event_id;
#endif

    LCI_CE_DEBUG_VERBOSE("Put Target handshake %s:\t%d -> %d(%p) size %zu with tag %d, cb data %p",
                         send_eager ? "eager" : "rendezvous",
                         handle->args.remote, ep_rank,
                         (void *)handle->args.msg, handle->args.size,
                         handle->args.tag, (void *)handle->args.data);

    LCI_HANDLER_PROGRESS(LCI_PUT_TARGET_HANDSHAKE);

    /* copy message to dest if it was sent eagerly */
    if (send_eager) {
        byte_t *msg = handshake->data + handshake->header.cb_size;
        memcpy(handshake->header.buffer, msg, handshake->header.size);
#if defined(PARSEC_PROF_TRACE_LCI)
        lci_profile(prog_prof, LCI_PUT_HS_ek, event_id);
        lci_profile_info(prog_prof, LCI_PUT_CB_sk, event_id,
                         handle->args.tag, handle->args.size, CB_SIZE_NULL,
                         handle->args.remote, ep_rank);
#endif /* PARSEC_PROF_TRACE_LCI */
        lci_dynamic_fifo_push(handle);
    } else {
        /* attempt to start rendezvous receive on target for put */
        lc_status ret = lc_recvl(handle->args.msg, handle->args.size,
                                 handle->args.remote, handle->args.tag,
                                put_ep, &handle->req);
        if (LC_OK == ret) {
            /* attempt succeeded */
            LCI_CE_DEBUG_VERBOSE("Put Target start:\t%d -> %d(%p) size %zu with tag %d",
                                 handle->args.remote, ep_rank,
                                 (void *)handle->args.msg, handle->args.size,
                                 handle->args.tag);
#if defined(PARSEC_PROF_TRACE_LCI)
            /* lci_put_target_handler is called on this same thread,
             * but necessarily only in lc_progress, so this ordering is safe */
            lci_profile(prog_prof, LCI_PUT_HS_ek, event_id);
            lci_profile_info(prog_prof, LCI_PUT_DATA_sk, event_id,
                             handle->args.tag, handle->args.size, CB_SIZE_NULL,
                             handle->args.remote, ep_rank);
#endif /* PARSEC_PROF_TRACE_LCI */
#ifdef PARSEC_LCI_MESSAGE_LIMIT
            /* starting recv, increment message count */
//          lci_message_count_inc();
#endif
        } else {
            /* attempt failed, delegate to comm thread */
            handle->args.type = LCI_PUT_TARGET_HANDSHAKE;
            lci_am_fifo_push(handle);
        }
    }
    /* push to callback fifo
     * if eager, comm thread will call callback
     * if not, comm thread will start receive */
#if 0
    parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
#endif
}

/* handler for put on target
 * using rendezvous recv, should only be called on progress thread */
static inline void lci_put_target_handler(lc_req *req)
{
    /* get callback handle from request, which points to handle->req */
    lci_cb_handle_t *handle = container_of(req, lci_cb_handle_t, req);
    assert(handle->args.type == LCI_PUT_TARGET && "wrong handle type");
    LCI_CE_DEBUG_VERBOSE("Put Target end:\t%d -> %d(%p) size %zu with tag %d",
                         handle->args.remote, ep_rank,
                         (void *)handle->args.msg, handle->args.size,
                         handle->args.tag);
#ifdef PARSEC_PROF_TRACE_LCI
    lci_profile(prog_prof, LCI_PUT_DATA_ek, handle->event_id);
    lci_profile_info(prog_prof, LCI_PUT_CB_sk, handle->event_id,
                     handle->args.tag, handle->args.size, CB_SIZE_NULL,
                     handle->args.remote, ep_rank);
#endif

    /* recv is always large, so this is always called on progress thread
     * push to back of progress cb fifo */
    LCI_HANDLER_PROGRESS(LCI_PUT_TARGET);
#if 0
    parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
#endif
    lci_dynamic_fifo_push(handle);
#if 0
    if (pthread_equal(progress_thread_id, pthread_self())) {
        LCI_HANDLER_PROGRESS(LCI_PUT_TARGET);
        /* recv was posted prior to send arrival, so we are on progress thread
         * push to back of progress cb fifo */
        parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
    } else {
        LCI_HANDLER_OTHER(LCI_PUT_TARGET);
        /* recv matched with unexpected send, so we are on communication thread
         * push to back of comm cb fifo */
        parsec_list_nolock_push_back(&lci_comm_cb_fifo, &handle->list_item);
    }
#endif
}

/* handler for get on origin
 * can be called anywhere - deal with progress thread vs. elsewhere */
static inline void lci_get_origin_handler(lc_req *req)
{
    /* get callback handle from request, which points to handle->req */
    lci_cb_handle_t *handle = container_of(req, lci_cb_handle_t, req);
    assert(handle->args.type == LCI_GET_ORIGIN && "wrong handle type");
    LCI_CE_DEBUG_VERBOSE("Get Origin end:\t%d(%p) <- %d(%p) size %zu with tag %d",
                         ep_rank,
                         (void *)((lci_mem_reg_handle_data_t *)handle->args.lreg)->mem,
                         handle->args.ldispl,
                         handle->args.remote,
                         (void *)((lci_mem_reg_handle_data_t *)handle->args.rreg)->mem,
                         handle->args.rdispl,
                         handle->args.size, req->meta);

    if (pthread_equal(progress_thread_id, pthread_self())) {
        LCI_HANDLER_PROGRESS(LCI_GET_ORIGIN);
        /* recv was posted prior to send arrival, so we are on progress thread
         * push to back of progress cb fifo */
#if 0
        parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
#endif
        lci_dynamic_fifo_push(handle);
    } else {
        LCI_HANDLER_OTHER(LCI_GET_ORIGIN);
        /* recv matched with unexpected send, so we are on caller thread
         * assume this is communication thread for now
         * push to back of comm cb fifo */
//      parsec_list_nolock_push_sorted(&lci_comm_cb_fifo, &handle->list_item,
//                                     CB_HANDLE_PRIORITY);
//      parsec_list_nolock_push_back(&lci_comm_cb_fifo, &handle->list_item);
//      parsec_list_push_back(&lci_shared_cb_fifo, &handle->list_item);
//      parsec_list_push_sorted(&lci_shared_cb_fifo, &handle->list_item,
//                              CB_HANDLE_PRIORITY);
        lci_dynamic_fifo_push(handle);
    }
}

/* handler for get handshake on target
 * only called from within lc_progress (i.e. on progress thread) */
static inline void lci_get_target_handshake_handler(lc_req *req)
{
    lci_handshake_t *handshake = req->buffer;

    lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
    handle->args.tag         = req->meta;
    handle->args.msg         = handshake->header.buffer;
    /* array to pointer decay for handshake->data */
    handle->args.data        = handshake->data;
    handle->args.size        = handshake->header.size;
    handle->args.remote      = req->rank;
    handle->args.type        = LCI_GET_TARGET_HANDSHAKE;
    handle->cb.get_target    = (parsec_ce_am_callback_t)handshake->header.cb;

    LCI_CE_DEBUG_VERBOSE("Get Target handshake:\t%d <- %d(%p) size %zu with tag %d, cb data %p",
                         handle->args.remote, ep_rank,
                         (void *)handle->args.msg, handle->args.size,
                         handle->args.tag, (void *)handle->args.data);

    LCI_HANDLER_PROGRESS(LCI_GET_TARGET_HANDSHAKE);
    /* push to callback fifo - send will be started by comm thread */
#if 0
    parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
#endif
    lci_am_fifo_push(handle);
}

/* handler for get on target
 * can be called anywhere - deal with progress thread vs. elsewhere */
static inline void lci_get_target_handler(void *ctx)
{
#if 0
#ifdef PARSEC_LCI_MESSAGE_LIMIT
    /* send is complete, decrement message count */
    lci_message_count_dec();
#endif
#endif
    /* ctx is pointer to lci_cb_handle_t */
    lci_cb_handle_t *handle = ctx;
    assert(handle->args.type == LCI_GET_TARGET && "wrong handle type");
    LCI_CE_DEBUG_VERBOSE("Get Target end:\t%d <- %d(%p) size %zu with tag %d",
                         handle->args.remote, ep_rank,
                         (void *)handle->args.msg, handle->args.size,
                         handle->args.tag);
    if (pthread_equal(progress_thread_id, pthread_self())) {
        LCI_HANDLER_PROGRESS(LCI_GET_TARGET);
        /* send was large, so we are on progress thread
         * push to back of progress cb fifo */
#if 0
        parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
#endif
        lci_dynamic_fifo_push(handle);
    } else {
        LCI_HANDLER_OTHER(LCI_GET_TARGET);
        /* send was small or medium, so we are on communication thread
         * push to back of comm cb fifo */
//      parsec_list_nolock_push_sorted(&lci_comm_cb_fifo, &handle->list_item,
//                                     CB_HANDLE_PRIORITY);
//      parsec_list_nolock_push_back(&lci_comm_cb_fifo, &handle->list_item);
//      parsec_list_push_back(&lci_shared_cb_fifo, &handle->list_item);
//      parsec_list_push_sorted(&lci_shared_cb_fifo, &handle->list_item,
//                              CB_HANDLE_PRIORITY);
        lci_dynamic_fifo_push(handle);
    }
}



/******************************************************************************
 *                    Communication Engine Implementation                     *
 *****************************************************************************/

/* bind progress thread to core */
static void progress_thread_bind(parsec_context_t *context, int binding)
{
#if defined(PARSEC_HAVE_HWLOC) && defined(PARSEC_HAVE_HWLOC_BITMAP)
    /* we weren't given a binding, so choose one from free mask */
    if (binding < 0) {
        binding = hwloc_bitmap_first(context->cpuset_free_mask);
        /* if this consumed last free core, make sure to bind comm thread here too */
        if (hwloc_bitmap_next(context->cpuset_free_mask, binding) < 0 &&
            context->comm_th_core < 0) {
            context->comm_th_core = binding;
        }
    }
#endif /* PARSEC_HAVE_HWLOC */

    if (binding >= 0) {
        /* we have a valid binding */
        if (parsec_bindthread(binding, -1) > -1) {
#if defined(PARSEC_HAVE_HWLOC) && defined(PARSEC_HAVE_HWLOC_BITMAP)
            /* we are alone if:
             *   1a: requested binding is not in allowed mask
             *       (so obviously not used by compute thread) OR
             *   1b: requested binding is in free mask
             *       (so obviously not used by anyone) AND
             *   2:  requested binding isn't the same as that of comm thread */
            if ((!hwloc_bitmap_isset(context->cpuset_allowed_mask, binding) ||
                  hwloc_bitmap_isset(context->cpuset_free_mask, binding)) &&
                context->comm_th_core != binding) {
                /* we are alone, disable yielding */
                lci_comm_yield = 0;
            } else {
                lci_ce_debug_verbose_lvl(4, "core %d hosts progress thread "
                                            "and compute eu or comm thread",
                                         binding);
            }
            /* set bit in allowed mask, clear in free mask */
            hwloc_bitmap_set(context->cpuset_allowed_mask, binding);
            hwloc_bitmap_clr(context->cpuset_free_mask, binding);
#else /* PARSEC_HAVE_HWLOC */
            /* we were given an explicit binding, presumably we're alone? */
            lci_comm_yield = 0;
#endif /* PARSEC_HAVE_HWLOC */
            lci_ce_debug_verbose_lvl(4, "progress thread bound "
                                        "to physical core %d (with%s backoff)",
                                     binding, (lci_comm_yield ? "" : "out"));


        } else {
#if !defined(PARSEC_OSX)
            /* might share a core */
            parsec_warning("Request to bind LCI progress thread on core %d failed.", binding);
#endif  /* !defined(PARSEC_OSX) */
        }
    } else {
#if defined(PARSEC_HAVE_HWLOC) && defined(PARSEC_HAVE_HWLOC_BITMAP)
        /* no binding given or no free core, let thread float */
        if (parsec_bindthread_mask(context->cpuset_allowed_mask) > -1) {
            char *mask_str = NULL;
            hwloc_bitmap_asprintf(&mask_str, context->cpuset_allowed_mask);
            lci_ce_debug_verbose_lvl(4, "progress thread bound "
                                        "on the cpu mask %s (with%s yielding)",
                                     mask_str, (lci_comm_yield ? "" : "out"));
            free(mask_str);
        }
#else /* PARSEC_HAVE_HWLOC */
        /* we don't even bother with this case for now, just use hwloc */
#endif /* PARSEC_HAVE_HWLOC */
    }
}

/* progress thread main function */
static void * lci_progress_thread(void *arg)
{
    parsec_list_item_t *ring = NULL;

    LCI_CE_DEBUG_VERBOSE("progress thread start");
    PARSEC_PAPI_SDE_THREAD_INIT();

    /* bind thread */
    parsec_context_t *context = arg;
    progress_thread_bind(context, progress_thread_binding);

#ifdef PARSEC_PROF_TRACE_LCI
    parsec_profiling_set_default_thread(prog_prof);
#endif

    /* signal init */
    pthread_mutex_lock(&progress_start.mutex);
    progress_start.status = true;
    pthread_cond_signal(&progress_start.cond);
    pthread_mutex_unlock(&progress_start.mutex);

    /* loop until told to stop */
    while (!atomic_load_explicit(&progress_thread_stop, memory_order_acquire)) {
        size_t progress_count = 0;
        /* progress until nothing progresses */
        while (lc_progress(0))
            progress_count++;

#if 0
        /* push back to shared callback fifo */
        if (NULL != (ring = parsec_list_nolock_unchain(&lci_progress_cb_fifo))) {
            parsec_list_chain_back(&lci_shared_cb_fifo, ring);
        }
#endif

#if 0
        /* did we do any work? */
        if (!parsec_list_nolock_is_empty(&lci_progress_cb_fifo)) {
            lci_fifo_try_chain();
        }
#endif

        /* sleep for comm_yield_ns if:
         *     lci_comm_yield == 1 && we made no progress in last loop
         *     lci_comm_yield == 2
         * else continue */
        const struct timespec ts = { .tv_sec = 0, .tv_nsec = comm_yield_ns };
        switch (lci_comm_yield) {
        case 1:
            /* if we made progress, continue for another loop */
            if (progress_count > 0)
                break;
            __attribute__((fallthrough));
        case 2:
            nanosleep(&ts, NULL);
            break;
        default:
            break;
        }
    }

#if 0
    _Bool stop = false;
    while (!stop) {
        pthread_mutex_lock(&progress_continue.mutex);
        switch (progress_continue.status) {
        case LCI_RUN:
            lc_progress(0);
            break;
        case LCI_PAUSE:
            pthread_cond_wait(&progress_continue.cond, &progress_continue.mutex);
            break;
        case LCI_STOP:
            stop = true;
            break;
        }
        pthread_mutex_unlock(&progress_continue.mutex);
    }
#endif

    /* make sure we release any claimed ticket */
    LCI_FIFO_LOCK_CLEAR(LCI_FIFO_LOCK_PROGRESS);

    progress_start.status = false;
    PARSEC_PAPI_SDE_THREAD_FINI();
    LCI_CE_DEBUG_VERBOSE("progress thread stop");
    return NULL;
}

/* Initialize LCI communication engine */
parsec_comm_engine_t *
lci_init(parsec_context_t *context)
{
    assert(-1 != context->comm_ctx);
    if (default_ep == (lc_ep *)context->comm_ctx) {
        return &parsec_ce;
    }

    /* set size and rank */
    lc_get_num_proc(&ep_size);
    lc_get_proc_num(&ep_rank);
    context->nb_nodes = ep_size;
    context->my_rank  = ep_rank;

    default_ep = (lc_ep *)context->comm_ctx;

    LCI_CE_DEBUG_VERBOSE("init");

    parsec_mca_param_reg_int_name("lci", "thread_bind",
                                  "Bind LCI progress thread to core",
                                  false, false,
                                  -1, &progress_thread_binding);
    lci_comm_yield = comm_yield;

#ifdef PARSEC_LCI_MESSAGE_LIMIT
    parsec_mca_param_reg_sizet_name("lci", "max_message",
                                    "Maximum number of concurrent messages to send",
                                    false, false,
                                    SIZE_MAX, &lci_max_message);
#endif

#ifdef PARSEC_PROF_TRACE_LCI
    lci_profiling_init();
#endif

    /* Make all the fn pointers point to this component's functions */
    parsec_ce.tag_register        = lci_tag_register;
    parsec_ce.tag_unregister      = lci_tag_unregister;
    parsec_ce.mem_register        = lci_mem_register;
    parsec_ce.mem_unregister      = lci_mem_unregister;
    parsec_ce.get_mem_handle_size = lci_get_mem_reg_handle_size;
    parsec_ce.mem_retrieve        = lci_mem_retrieve;
    parsec_ce.put                 = lci_put;
    parsec_ce.get                 = lci_get;
    parsec_ce.progress            = lci_cb_progress;
    parsec_ce.enable              = lci_enable;
    parsec_ce.disable             = lci_disable;
    parsec_ce.pack                = lci_pack;
    parsec_ce.unpack              = lci_unpack;
    parsec_ce.reshape             = lci_reshape;
    parsec_ce.sync                = lci_sync;
    parsec_ce.can_serve           = lci_can_push_more;
    parsec_ce.send_am             = lci_send_am;
    parsec_ce.parsec_context = context;
    parsec_ce.capabilites.sided = 2;
    parsec_ce.capabilites.supports_noncontiguous_datatype = 0;

    /* create a mempool for memory registration */
    parsec_mempool_construct(&lci_mem_reg_handle_mempool,
                             PARSEC_OBJ_CLASS(lci_mem_reg_handle_t),
                             sizeof(lci_mem_reg_handle_t),
                             offsetof(lci_mem_reg_handle_t, mempool_owner),
                             1);
    /* set alignment to 2^6 = 64 bytes */
    for (size_t i = 0; i < lci_mem_reg_handle_mempool.nb_thread_mempools; i++) {
        lci_mem_reg_handle_mempool.thread_mempools[i].mempool.alignment = 6;
    }

    /* create a mempool for callbacks */
    parsec_mempool_construct(&lci_cb_handle_mempool,
                             PARSEC_OBJ_CLASS(lci_cb_handle_t),
                             sizeof(lci_cb_handle_t),
                             offsetof(lci_cb_handle_t, mempool_owner),
                             1);
    /* set alignment to 2^8 = 256 bytes */
    for (size_t i = 0; i < lci_cb_handle_mempool.nb_thread_mempools; i++) {
        lci_cb_handle_mempool.thread_mempools[i].mempool.alignment = 8;
    }

    /* create a mempool for dynamic messages */
    parsec_mempool_construct(&lci_dynmsg_mempool,
                             PARSEC_OBJ_CLASS(lci_dynmsg_t),
                             /* ensure enough space for any medium message */
                             sizeof(lci_dynmsg_t) + lc_max_medium(0),
                             offsetof(lci_dynmsg_t, mempool_owner),
                             1);
    /* set alignment to 2^7 = 128 bytes */
    for (size_t i = 0; i < lci_dynmsg_mempool.nb_thread_mempools; i++) {
        lci_dynmsg_mempool.thread_mempools[i].mempool.alignment = 7;
    }

    /* create send callback queues */
    PARSEC_OBJ_CONSTRUCT(&lci_shared_cb_fifo, parsec_list_t);
    PARSEC_OBJ_CONSTRUCT(&lci_progress_cb_fifo, parsec_list_t);
    PARSEC_OBJ_CONSTRUCT(&lci_comm_cb_fifo, parsec_list_t);

    PARSEC_OBJ_CONSTRUCT(&lci_am_fifo, parsec_list_t);
    PARSEC_OBJ_CONSTRUCT(&lci_dynamic_fifo, parsec_list_t);

#ifdef PARSEC_LCI_CB_HASH_TABLE
    /* allocated hash tables for callbacks */
    PARSEC_OBJ_CONSTRUCT(&lci_am_cb_hash_table, parsec_hash_table_t);
    parsec_hash_table_init(&lci_am_cb_hash_table,
                           offsetof(lci_am_reg_handle_t, ht_item),
                           4, lci_am_key_fns, &lci_am_cb_hash_table);
#else /* PARSEC_LCI_CB_HASH_TABLE */
    for (size_t i = 0; i < LCI_AM_CB_TAG_MAX; i++) {
        lci_am_cb_array[i].data = NULL;
        lci_am_cb_array[i].cb = NULL;
    }
#endif /* PARSEC_LCI_CB_HASH_TABLE */

    /* init LCI */
    lc_opt opt = { .dev = 0 };

    /* collective endpoint */
    opt.desc = LC_EXP_SYNC;
    lc_ep_dup(&opt, *default_ep, &collective_ep);

    /* active message endpoint */
    opt.desc = LC_DYN_AM;
    opt.alloc = lci_dyn_alloc;
    opt.handler = lci_active_message_handler;
    lc_ep_dup(&opt, *default_ep, &am_ep);

    /* one-sided AM endpoint */
    opt.handler = lci_put_target_handshake_handler;
    lc_ep_dup(&opt, *default_ep, &put_am_ep);
    opt.handler = lci_get_target_handshake_handler;
    lc_ep_dup(&opt, *default_ep, &get_am_ep);

    /* abort endpoint */
    opt.handler = lci_abort_handler;
    lc_ep_dup(&opt, *default_ep, &abort_ep);

    /* one-sided endpoint */
    opt.desc = LC_EXP_AM;
    opt.alloc = NULL;
    opt.handler = lci_put_target_handler;
    lc_ep_dup(&opt, *default_ep, &put_ep);
    opt.handler = lci_get_origin_handler;
    lc_ep_dup(&opt, *default_ep, &get_ep);

    /* start progress thread if multiple nodes */
    if (ep_size > 1) {
        LCI_CE_DEBUG_VERBOSE("starting progress thread");
        /* lock mutex before starting thread */
        pthread_mutex_lock(&progress_start.mutex);
        /* ensure progress_thread_stop == false */
        atomic_store_explicit(&progress_thread_stop, false, memory_order_release);
        /* start thread */
        pthread_create(&progress_thread_id, NULL, lci_progress_thread, context);
        /* wait until thread started */
        while (!progress_start.status)
            pthread_cond_wait(&progress_start.cond, &progress_start.mutex);
        /* unlock mutex */
        pthread_mutex_unlock(&progress_start.mutex);
    }

    return &parsec_ce;
}

/* Finalize LCI communication engine */
int
lci_fini(parsec_comm_engine_t *comm_engine)
{
    LCI_CE_DEBUG_VERBOSE("fini");
    lci_sync(comm_engine);
    void *progress_retval = NULL;

    /* make sure we release any claimed ticket */
    LCI_FIFO_LOCK_CLEAR(LCI_FIFO_LOCK_COMM);

    /* stop progress thread if multiple nodes */
    if (ep_size > 1) {
        LCI_CE_DEBUG_VERBOSE("stopping progress thread");
        atomic_store_explicit(&progress_thread_stop, true, memory_order_release);
#if 0
        pthread_mutex_lock(&progress_continue.mutex);
        progress_continue.status = LCI_STOP;
        pthread_cond_signal(&progress_continue.cond); /* if paused */
        pthread_mutex_unlock(&progress_continue.mutex);
#endif
        pthread_join(progress_thread_id, &progress_retval);
    }

#ifdef PARSEC_LCI_CB_HASH_TABLE
    parsec_hash_table_fini(&lci_am_cb_hash_table);
    PARSEC_OBJ_DESTRUCT(&lci_am_cb_hash_table);
#endif

    PARSEC_OBJ_DESTRUCT(&lci_comm_cb_fifo);
    PARSEC_OBJ_DESTRUCT(&lci_progress_cb_fifo);
    PARSEC_OBJ_DESTRUCT(&lci_shared_cb_fifo);

    PARSEC_OBJ_DESTRUCT(&lci_am_fifo);
    PARSEC_OBJ_DESTRUCT(&lci_dynamic_fifo);

    parsec_mempool_destruct(&lci_dynmsg_mempool);
    parsec_mempool_destruct(&lci_cb_handle_mempool);
    parsec_mempool_destruct(&lci_mem_reg_handle_mempool);

    return 1;
}

int lci_tag_register(parsec_ce_tag_t tag,
                     parsec_ce_am_callback_t cb,
                     void *cb_data,
                     size_t msg_length)
{
    LCI_CE_DEBUG_VERBOSE("register Active Message %"PRIu64" data %p size %zu",
                         tag, cb_data, msg_length);
    if (msg_length > lc_max_medium(0)) {
        parsec_warning("LCI[%d]:\tActive Message %"PRIu64": size %zu >  max %zu",
                       ep_rank, tag, msg_length, lc_max_medium(0));
        return PARSEC_ERROR;
    }

#ifdef PARSEC_LCI_CB_HASH_TABLE
    parsec_key_t key = tag;
    /* allocate handle */
    lci_am_reg_handle_t *handle = malloc(sizeof(*handle));
    handle->ht_item.key = key;
    handle->data = cb_data;
    handle->cb   = cb;

    parsec_hash_table_lock_bucket(&lci_am_cb_hash_table, key);
    if (NULL != parsec_hash_table_nolock_find(&lci_am_cb_hash_table, key)) {
        parsec_warning("LCI[%d]:\tActive Message %"PRIu64" already registered",
                       ep_rank, tag);
        parsec_hash_table_unlock_bucket(&lci_am_cb_hash_table, key);
        return PARSEC_EXISTS;
    }

    parsec_hash_table_nolock_insert(&lci_am_cb_hash_table, &handle->ht_item);
    parsec_hash_table_unlock_bucket(&lci_am_cb_hash_table, key);
    return PARSEC_SUCCESS;

#else /* PARSEC_LCI_CB_HASH_TABLE */
    assert(tag < AM_CB_TAG_MAX && "tag too large");
    if (NULL != lci_am_cb_array[tag].cb) {
        parsec_warning("LCI[%d]:\tActive Message %"PRIu64" already registered",
                       ep_rank, tag);
        return PARSEC_EXISTS;
    }
    lci_am_cb_array[tag].data = cb_data;
    lci_am_cb_array[tag].cb   = cb;
    return PARSEC_SUCCESS;

#endif /* PARSEC_LCI_CB_HASH_TABLE */
}

int
lci_tag_unregister(parsec_ce_tag_t tag)
{
    LCI_CE_DEBUG_VERBOSE("unregister Active Message %"PRIu64, tag);
#ifdef PARSEC_LCI_CB_HASH_TABLE
    parsec_key_t key = tag;
    lci_am_reg_handle_t *handle = parsec_hash_table_remove(&lci_am_cb_hash_table, key);
    if (NULL == handle) {
        parsec_warning("LCI[%d]:\tActive Message %"PRIu64" not registered", ep_rank, tag);
        return 0;
    }
    free(handle);
    return 1;

#else /* PARSEC_LCI_CB_HASH_TABLE */
    assert(tag < AM_CB_TAG_MAX && "tag too large");
    if (NULL == lci_am_cb_array[tag].cb) {
        parsec_warning("LCI[%d]:\tActive Message %"PRIu64" not registered", ep_rank, tag);
        return 0;
    }
    lci_am_cb_array[tag].data = NULL;
    lci_am_cb_array[tag].cb   = NULL;
    return 1;

#endif /* PARSEC_LCI_CB_HASH_TABLE */
}

/* Register memory for use with LCI */
int
lci_mem_register(void *mem, parsec_mem_type_t mem_type,
                 size_t count, parsec_datatype_t datatype,
                 size_t mem_size,
                 parsec_ce_mem_reg_handle_t *lreg,
                 size_t *lreg_size)
{
    /* LCI only supports contiguous types */
    assert(mem_type == PARSEC_MEM_TYPE_CONTIGUOUS && "only supports contiguous memory");

    LCI_CE_DEBUG_VERBOSE("register memory %p size %zu", mem, mem_size);

    /* allocate from mempool */
    lci_mem_reg_handle_t *handle = parsec_thread_mempool_allocate(
                                   lci_mem_reg_handle_mempool.thread_mempools);
    /* set mem handle info */
    handle->reg.mem      = mem;
    handle->reg.size     = mem_size;
    handle->reg.count    = count;
    handle->reg.datatype = datatype;

    /* register memory with LCI for put/get */
    //LCI_register(mem, mem_size);

    /* return the pointer to the handle data and its size */
    *lreg = (parsec_ce_mem_reg_handle_t)&handle->reg;
    *lreg_size = sizeof(lci_mem_reg_handle_data_t);
    return 1;
}

int
lci_mem_unregister(parsec_ce_mem_reg_handle_t *lreg)
{
    /* *lreg points to lci_mem_reg_handle_t::reg */
    lci_mem_reg_handle_t *handle = container_of(*lreg, lci_mem_reg_handle_t, reg);
    LCI_CE_DEBUG_VERBOSE("unregister memory %p size %zu",
                         (void *)handle->reg.mem, handle->reg.size);
    //LCI_unregister(handle->mem);
    /* we clobber the class system object fields due to union, fix it! */
    PARSEC_OBJ_CONSTRUCT(handle, lci_mem_reg_handle_t);
    parsec_mempool_free(&lci_mem_reg_handle_mempool, handle);
    *lreg = NULL;
    return 1;
}

/* Size of an LCI memory handle */
int
lci_get_mem_reg_handle_size(void)
{
    return sizeof(lci_mem_reg_handle_data_t);
}

int
lci_mem_retrieve(parsec_ce_mem_reg_handle_t lreg,
                 void **mem, parsec_datatype_t *datatype, int *count)
{
    lci_mem_reg_handle_data_t *handle_data = (lci_mem_reg_handle_data_t *)lreg;
    *mem      = handle_data->mem;
    *count    = handle_data->count;
    *datatype = handle_data->datatype;
    LCI_CE_DEBUG_VERBOSE("retrieve memory %p size %zu",
                         (void *)handle_data->mem, handle_data->size);
    return 1;
}

int
lci_put(parsec_comm_engine_t *comm_engine,
        parsec_ce_mem_reg_handle_t lreg,
        ptrdiff_t ldispl,
        parsec_ce_mem_reg_handle_t rreg,
        ptrdiff_t rdispl,
        size_t size,
        int remote,
        parsec_ce_onesided_callback_t l_cb, void *l_cb_data,
        parsec_ce_tag_t r_tag, void *r_cb_data, size_t r_cb_data_size)
{
    lci_mem_reg_handle_data_t *ldata = (lci_mem_reg_handle_data_t *)lreg;
    lci_mem_reg_handle_data_t *rdata = (lci_mem_reg_handle_data_t *)rreg;
    assert(ldata->size <= rdata->size && "put origin buffer larger than target");

    size_t buffer_size = sizeof(lci_handshake_t) + r_cb_data_size;
    size_t buffer_size_eager = buffer_size + ldata->size;
    assert(buffer_size <= lc_max_medium(0) && "active message data too long");

    bool send_eager = LCI_ENABLE_EAGER && (buffer_size_eager <= lc_max_medium(0));
    if (send_eager) {
        buffer_size = buffer_size_eager;
    }

    bool send_short = (buffer_size <= lc_max_short(0));
    bool use_stack = (buffer_size <= HANDSHAKE_STACK_BUFFER_SIZE);

    alignas(lci_handshake_t) byte_t stack_buffer[HANDSHAKE_STACK_BUFFER_SIZE];
    lci_dynmsg_t *dyn_buffer = NULL;

    lci_handshake_t *handshake = (lci_handshake_t *)&stack_buffer;
    if (!use_stack) {
        dyn_buffer = parsec_thread_mempool_allocate(
                                           lci_dynmsg_mempool.thread_mempools);
        handshake = (lci_handshake_t *)&dyn_buffer->data;
    }

    /* get next tag */
    int tag = lci_get_tag();

    void *lbuf = ldata->mem + ldispl;
    void *rbuf = rdata->mem + rdispl;

    // NOTE: size is always passed as 0, use ldata->size and rdata->size
    // NOTE: just use ldata->size, can send a buffer shorter than destination

    /* set handshake info */
    handshake->header.buffer  = rbuf;
    handshake->header.size    = ldata->size;
    handshake->header.cb_size = r_cb_data_size;
    handshake->header.cb      = r_tag;
    memcpy(handshake->data, r_cb_data, r_cb_data_size);
    if (send_eager) {
        memcpy(handshake->data + r_cb_data_size, lbuf, ldata->size);
    }

    /* send handshake to remote, will be retrieved from queue */
    LCI_CE_DEBUG_VERBOSE("Put Origin handshake %s:\t%d(%p+%td) -> %d(%p+%td) size %zu with tag %d",
                         send_eager ? "eager" : "rendezvous",
                         ep_rank, (void *)ldata->mem, ldispl,
                         remote,  (void *)rdata->mem, rdispl,
                         ldata->size, tag);
    /* use short send if small enough (true most of the time) */
    if (send_short) {
        RETRY(lc_sends(handshake, buffer_size, remote, tag, put_am_ep));
    } else {
        RETRY(lc_sendm(handshake, buffer_size, remote, tag, put_am_ep));
    }

    /* return dynamic memory to mempool */
    if (!use_stack) {
        parsec_mempool_free(&lci_dynmsg_mempool, dyn_buffer);
    }

    /* allocate from mempool */
    lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
    handle->args.lreg        = lreg;
    handle->args.ldispl      = ldispl;
    handle->args.rreg        = rreg;
    handle->args.rdispl      = rdispl;
    handle->args.data        = l_cb_data;
    handle->args.size        = ldata->size;
    handle->args.remote      = remote;
    handle->args.type        = LCI_PUT_ORIGIN;
    handle->cb.put_origin    = l_cb;

    if (send_eager) {
#if 0
        /* NOTE: lci_cb_progress assumes that this is a long send and
         * decrements the message count appropriately; we need to either
         * increment the count here or just execute the callback directly,
         * or figure out a way to disambiguate eager versus rendezvous in
         * lci_cb_progress. I'm not sure it makes a significant difference. */

        /* data sent eagerly in handshake, push to comm cb fifo
         * assume this is communication thread for now */
//      parsec_list_nolock_push_sorted(&lci_comm_cb_fifo,  &handle->list_item,
//                                     CB_HANDLE_PRIORITY);
        parsec_list_nolock_push_back(&lci_comm_cb_fifo,  &handle->list_item);
#else
        lci_put_origin_callback(handle, comm_engine);
#endif
    } else {
        LCI_CE_DEBUG_VERBOSE("Put Origin start:\t%d(%p+%td) -> %d(%p+%td) size %zu with tag %d",
                             ep_rank, (void *)ldata->mem, ldispl,
                             remote,  (void *)rdata->mem, rdispl,
                             ldata->size, tag);
#ifdef PARSEC_LCI_MESSAGE_LIMIT
        /* starting send, increment message count */
        lci_message_count_inc();
#endif
        /* start rendezvous send to remote with tag */
        RETRY(lc_sendl(lbuf, ldata->size, remote, tag, put_ep,
                      lci_put_origin_handler, handle));
    }
    LCI_CALL_PUT();
    return 1;
}

int
lci_get(parsec_comm_engine_t *comm_engine,
        parsec_ce_mem_reg_handle_t lreg,
        ptrdiff_t ldispl,
        parsec_ce_mem_reg_handle_t rreg,
        ptrdiff_t rdispl,
        size_t size,
        int remote,
        parsec_ce_onesided_callback_t l_cb, void *l_cb_data,
        parsec_ce_tag_t r_tag, void *r_cb_data, size_t r_cb_data_size)
{
    lci_mem_reg_handle_data_t *ldata = (lci_mem_reg_handle_data_t *)lreg;
    lci_mem_reg_handle_data_t *rdata = (lci_mem_reg_handle_data_t *)rreg;

    size_t buffer_size = sizeof(lci_handshake_t) + r_cb_data_size;
    assert(buffer_size <= lc_max_medium(0) && "active message data too long");
    assert(buffer_size <= HANDSHAKE_STACK_BUFFER_SIZE && "active message data too long");

    alignas(lci_handshake_t) byte_t buffer[HANDSHAKE_STACK_BUFFER_SIZE];
    lci_handshake_t *handshake = (lci_handshake_t *)&buffer;

    bool send_short = (buffer_size <= lc_max_short(0));

    /* get next tag */
    int tag = lci_get_tag();

    void *lbuf = ldata->mem + ldispl;
    void *rbuf = rdata->mem + rdispl;

    // NOTE: size is always passed as 0, use ldata->size and rdata->size

    /* set handshake info */
    handshake->header.buffer  = rbuf;
    handshake->header.size    = rdata->size;
    handshake->header.cb_size = r_cb_data_size;
    handshake->header.cb      = r_tag;
    memcpy(handshake->data, r_cb_data, r_cb_data_size);

    /* send handshake to remote, will be retrieved from queue */
    LCI_CE_DEBUG_VERBOSE("Get Origin handshake:\t%d(%p+%td) <- %d(%p+%td) size %zu with tag %d",
                         ep_rank, (void *)ldata->mem, ldispl,
                         remote,  (void *)rdata->mem, rdispl,
                         ldata->size, tag);
    /* use short send if small enough (true most of the time) */
    if (send_short) {
        RETRY(lc_sends(handshake, buffer_size, remote, tag, get_am_ep));
    } else {
        RETRY(lc_sendm(handshake, buffer_size, remote, tag, get_am_ep));
    }

    /* allocate from mempool */
    lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
    handle->args.lreg        = lreg;
    handle->args.ldispl      = ldispl;
    handle->args.rreg        = rreg;
    handle->args.rdispl      = rdispl;
    handle->args.data        = l_cb_data;
    handle->args.size        = ldata->size;
    handle->args.remote      = remote;
    handle->args.type        = LCI_GET_ORIGIN;
    handle->cb.get_origin    = l_cb;

    /* start recieve from remote with tag */
    LCI_CE_DEBUG_VERBOSE("Get Origin start:\t%d(%p+%td) <- %d(%p+%td) size %zu with tag %d",
                         ep_rank, (void *)ldata->mem, ldispl,
                         remote,  (void *)rdata->mem, rdispl,
                         ldata->size, tag);
#ifdef PARSEC_LCI_MESSAGE_LIMIT
    /* starting recv, increment message count */
//  lci_message_count_inc();
#endif
    RETRY(lc_recv(lbuf, ldata->size, remote, tag, get_ep, &handle->req));
    LCI_CALL_GET();
    return 1;
}

int
lci_send_am(parsec_comm_engine_t *comm_engine,
            parsec_ce_tag_t tag,
            int remote,
            void *addr, size_t size)
{
    assert(size <= lc_max_medium(0) && "active message data too long");
    LCI_CE_DEBUG_VERBOSE("Active Message %"PRIu64" send:\t%d -> %d with message %p size %zu",
                         tag, ep_rank, remote, addr, size);
    bool send_short = (size <= lc_max_short(0));
    if (send_short) {
        RETRY(lc_sends(addr, size, remote, tag, am_ep));
    } else {
        RETRY(lc_sendm(addr, size, remote, tag, am_ep));
    }
    LCI_CALL_AM();
    return 1;
}

_Noreturn void
lci_abort(int exit_code)
{
    LCI_CE_DEBUG_VERBOSE("Abort %d", exit_code);
    for (int i = 0; i < ep_size; i++) {
        if (i != ep_rank) {
            /* send abort message to all other processes */
            LCI_CE_DEBUG_VERBOSE("Abort %d send:\t%d -> %d", exit_code, ep_rank, i);
            RETRY(lc_sends(NULL, 0, i, exit_code, abort_ep));
        }
    }
    LCI_CE_DEBUG_VERBOSE("Abort %d barrier:\t%d ->", exit_code, ep_rank);
    /* wait for all processes to ack the abort */
    lc_barrier(collective_ep);
    /* exit without cleaning up */
    _Exit(exit_code);
}

static inline __attribute__((always_inline)) size_t
lci_process_cb_handle(lci_cb_handle_t *handle, parsec_comm_engine_t *comm_engine)
{
    lci_cb_handle_type_t type = handle->args.type;
    switch (type) {
    case LCI_ABORT:
        LCI_CE_DEBUG_VERBOSE("Abort %d barrier:\t%d ->",
                             (int)handle->args.tag, handle->args.remote);
        /* wait for all processes to ack the abort */
        lc_barrier(collective_ep);
        /* exit without cleaning up */
        _Exit(handle->args.tag);
        return 0;

    case LCI_ACTIVE_MESSAGE:
#ifdef PARSEC_PROF_TRACE_LCI
        lci_profile(comm_prof, LCI_AM_RCV_ek, handle->event_id);
#endif
        lci_active_message_callback(handle, comm_engine);
        return 0;

    case LCI_PUT_ORIGIN:
        lci_put_origin_callback(handle, comm_engine);
        return 1;

    case LCI_PUT_TARGET_HANDSHAKE:
        /* change handle type; we are now starting to put target call */
        handle->args.type = LCI_PUT_TARGET;
        /* start rendezvous receive on target for put */
        LCI_CE_DEBUG_VERBOSE("Put Target start:\t%d -> %d(%p) size %zu with tag %d",
                             handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);
#ifdef PARSEC_PROF_TRACE_LCI
        lci_profile(comm_prof, LCI_PUT_HS_ek, handle->event_id);
        lci_profile_info(comm_prof, LCI_PUT_DATA_sk, handle->event_id,
                         handle->args.tag, handle->args.size, CB_SIZE_NULL,
                         handle->args.remote, ep_rank);
#endif
#ifdef PARSEC_LCI_MESSAGE_LIMIT
        /* starting recv, increment message count */
//      lci_message_count_inc();
#endif
        RETRY(lc_recvl(handle->args.msg, handle->args.size,
                       handle->args.remote, handle->args.tag,
                       put_ep, &handle->req));
        return 0;

    case LCI_PUT_TARGET:
#ifdef PARSEC_PROF_TRACE_LCI
        lci_profile(comm_prof, LCI_PUT_CB_ek, handle->event_id);
#endif
        lci_put_target_callback(handle, comm_engine);
//      return 1;
        return 0;

    case LCI_GET_ORIGIN:
        lci_get_origin_callback(handle, comm_engine);
//      return 1;
        return 0;

    case LCI_GET_TARGET_HANDSHAKE:
        /* change handle type; we are now starting to get target call */
        handle->args.type = LCI_GET_TARGET;
        /* start send on target for get */
        LCI_CE_DEBUG_VERBOSE("Get Target start:\t%d <- %d(%p) size %zu with tag %d",
                             handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);
#ifdef PARSEC_LCI_MESSAGE_LIMIT
        /* starting send, increment message count */
        lci_message_count_inc();
#endif
        RETRY(lc_send(handle->args.msg, handle->args.size,
                      handle->args.remote, handle->args.tag,
                      get_ep, lci_get_target_handler, handle));
        return 0;

    case LCI_GET_TARGET:
        lci_get_target_callback(handle, comm_engine);
        return 1;

    default:
        lci_ce_fatal("invalid callback type: %d", type);
        return 0;
    }
}

static inline __attribute__((always_inline)) void
lci_process_cb_handle_fifo(parsec_list_t *fifo, int limit, int *progress,
                           size_t *completed, parsec_comm_engine_t *comm_engine)
{
#ifdef PARSEC_PROF_TRACE_LCI
    lci_prof_cb_prog_t prof_info;
#endif

    int iter = 0;
    do {
#if 1
        while (!parsec_atomic_trylock(&fifo->atomic_lock))
            continue;
        parsec_list_item_t *item = parsec_list_nolock_pop_front(fifo);
        parsec_atomic_unlock(&fifo->atomic_lock);
#else
        parsec_list_item_t *item = parsec_list_pop_front(fifo);
#endif
        if (NULL == item)
            break;

        lci_cb_handle_t *handle = container_of(item, lci_cb_handle_t, list_item);
        lci_cb_handle_type_t type = handle->args.type;
#ifdef PARSEC_PROF_TRACE_LCI
        prof_info.queue_len = atomic_fetch_sub_explicit(&queue_len, 1, memory_order_relaxed) - 1;
        prof_info.iter      = *progress + iter;
        PARSEC_PROFILING_TRACE(comm_prof, LCI_CB_PROG_sk, type, PROFILE_OBJECT_ID_NULL, &prof_info);
#endif
        (*completed) += lci_process_cb_handle(handle, comm_engine);
#ifdef PARSEC_PROF_TRACE_LCI
        lci_profile(comm_prof, LCI_CB_PROG_ek, type);
#endif
        iter++;
    } while ((limit ? iter < limit : 1));
    (*progress) += iter;
}

int
lci_cb_progress(parsec_comm_engine_t *comm_engine)
{
//  parsec_list_item_t *ring = NULL;
    size_t completed_messages = 0;
    int progress = 0;
    int prior_progress = 0;

#if 0
    ring = lci_fifo_try_unchain();
    /* if ring is NULL, we either didn't get lock or shared fifo was empty */
    if (NULL == ring) {
        return 0;
    }

    /* chain back, guaranteed ring is not NULL */
    parsec_list_nolock_chain_back(&lci_comm_cb_fifo, ring);
    for (parsec_list_item_t *item = parsec_list_nolock_pop_front(&lci_comm_cb_fifo);
                             item != NULL;
                             item = parsec_list_nolock_pop_front(&lci_comm_cb_fifo)) {
#endif
    do {
        completed_messages = 0;
        prior_progress = progress;
        lci_process_cb_handle_fifo(&lci_am_fifo,      5, &progress, &completed_messages, comm_engine);
        lci_process_cb_handle_fifo(&lci_dynamic_fifo, 0, &progress, &completed_messages, comm_engine);
        lci_message_count_sub(completed_messages);
    } while (progress > prior_progress);

    return progress;
}

/* restarts progress thread, if paused */
int
lci_enable(parsec_comm_engine_t *comm_engine)
{
#if 0
    int ret;
    pthread_mutex_lock(&progress_continue.mutex);
    if (ret = (progress_continue.status == LCI_PAUSE)) {
        progress_continue.status = LCI_RUN;
        pthread_cond_signal(&progress_continue.cond);
    }
    pthread_mutex_unlock(&progress_continue.mutex);
    return ret;
#endif
    return 1;
}

/* pauses progress thread, if running */
int
lci_disable(parsec_comm_engine_t *comm_engine)
{
#if 0
    int ret;
    pthread_mutex_lock(&progress_continue.mutex);
    if (ret = (progress_continue.status == LCI_RUN)) {
        progress_continue.status = LCI_PAUSE;
        pthread_cond_signal(&progress_continue.cond);
    }
    pthread_mutex_unlock(&progress_continue.mutex);
    return ret;
#endif
    return 1;
}

int
lci_pack(parsec_comm_engine_t *comm_engine,
         void *inbuf, int incount,
         void *outbuf, int outsize,
         int *position)
{
    LCI_CE_DEBUG_VERBOSE("pack %p(%d) into %p(%d) + %d",
                         inbuf, incount, outbuf, outsize, *position);
    /* what's the behavior when outbuf overflows? MPI_Pack doesn't say lol */
    assert(*position + incount <= outsize && "pack overflow");
    int remaining = outsize - *position;
    if (incount > remaining)
        incount = remaining;
    byte_t *in = inbuf;
    byte_t *out = (byte_t *)outbuf + (ptrdiff_t)*position;
    memcpy(out, in, incount);
    *position += incount;
    return 1;
}

int
lci_unpack(parsec_comm_engine_t *comm_engine,
           void *inbuf, int insize, int *position,
           void *outbuf, int outcount)
{
    LCI_CE_DEBUG_VERBOSE("unpack %p(%d) + %d into %p(%d)",
                         inbuf, insize, *position, outbuf, outcount);
    /* what happens if we try to unpack more than is available? */
    assert(*position + outcount <= insize && "unpack overflow");
    int remaining = insize - *position;
    if (outcount > remaining)
        outcount = remaining;
    byte_t *in = (byte_t *)inbuf + (ptrdiff_t)*position;
    byte_t *out = outbuf;
    memcpy(out, in, outcount);
    *position += outcount;
    return 1;
}

int
lci_reshape(parsec_comm_engine_t *comm_engine,
            parsec_execution_stream_t* es,
            parsec_data_copy_t *dst,
            parsec_data_copy_t *src,
            parsec_datatype_t layout,
            int64_t displ_src,
            int64_t displ_dst,
            uint64_t count)
{
    int size;
    int rc = parsec_type_size(layout, &size);
    if (rc != PARSEC_SUCCESS)
        return 0;
    size_t bytes = size * count;
    uint8_t *dst_buf = (uint8_t *)PARSEC_DATA_COPY_GET_PTR(dst) + displ_dst;
    uint8_t *src_buf = (uint8_t *)PARSEC_DATA_COPY_GET_PTR(src) + displ_src;
    LCI_CE_DEBUG_VERBOSE("reshape %p into %p: %"PRIu64" x datatype(%p)",
                         src_buf, dst_buf, count, (void *)layout);
    memcpy(dst_buf, src_buf, bytes);
    return 1;
}

int
lci_sync(parsec_comm_engine_t *comm_engine)
{
    LCI_CE_DEBUG_VERBOSE("sync");
    lc_barrier(collective_ep);
    return 1;
}

int
lci_can_push_more(parsec_comm_engine_t *comm_engine)
{
#ifdef PARSEC_LCI_MESSAGE_LIMIT
    /* check if we can send more messages */
    size_t message_count = lci_message_count_load();
    return message_count < lci_max_message;
#else
    return 1;
#endif
}
