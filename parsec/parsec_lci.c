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
#include "parsec/execution_stream.h"
#include "parsec/class/list_item.h"
#include "parsec/class/parsec_hash_table.h"
#include "parsec/class/parsec_object.h"
#include "parsec/mempool.h"
#include "parsec/parsec_comm_engine.h"
#include "parsec/parsec_internal.h"
#include "parsec/parsec_lci.h"
#include "parsec/parsec_remote_dep.h"
#include "parsec/utils/debug.h"

/* ------- LCI implementation below ------- */

typedef unsigned char byte_t;

#define lci_ce_debug_verbose(FMT, ...)                                        \
  do {                                                                        \
    parsec_debug_verbose(20, parsec_debug_output,                             \
                         "LCI[%d]:\t" FMT,                                    \
                         ep_rank, ##__VA_ARGS__);                             \
  } while (0)

#define lci_ce_fatal(FMT, ...)                                                \
  do {                                                                        \
    parsec_fatal("LCI[%d]:\t" FMT, ep_rank, ##__VA_ARGS__);                   \
  } while (0)

#define NS_PER_S 1000000000L
//#define RETRY(lci_call) do { } while (LC_OK != (lci_call))
#define RETRY(lci_call)                                                       \
  do {                                                                        \
    struct timespec _ts = { .tv_sec = 0, .tv_nsec = 1 };                      \
    lc_status _status = LC_OK;                                                \
    for (long _ns = 0; _ns < NS_PER_S; _ns += _ts.tv_nsec) {                  \
        lci_ce_debug_verbose(#lci_call);                                      \
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
typedef struct lci_mem_reg_handle_s {
    alignas(64) parsec_list_item_t list_item; /* for mempool */
    parsec_thread_mempool_t        *mempool_owner;
    byte_t                         *mem;
    //byte_t                         *packed; /* pack buffer for noncontigiguous dt */
    size_t                         size;
    size_t                         count;
    parsec_datatype_t              datatype;
} lci_mem_reg_handle_t;
static OBJ_CLASS_INSTANCE(lci_mem_reg_handle_t, parsec_list_item_t, NULL, NULL);

/* memory pool for memory handles */
static parsec_mempool_t lci_mem_reg_handle_mempool;

/* types of callbacks */
typedef enum lci_cb_handle_type_e {
    LCI_ABORT,
    LCI_ACTIVE_MESSAGE,
    LCI_PUT_ORIGIN,
    LCI_PUT_TARGET,
    LCI_GET_ORIGIN,
    LCI_GET_TARGET,
} lci_cb_handle_type_t;

/* LCI callback hash table type - 128 bytes */
typedef struct lci_cb_handle_s {
    alignas(128) parsec_list_item_t       list_item; /* pool/queue: 32 bytes */
    alignas(32)  parsec_hash_table_item_t ht_item;   /* hash table: 24 bytes */
    alignas(8)   parsec_thread_mempool_t  *mempool_owner;        /*  8 bytes */
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
                lc_req               *req; /* request, for deferred recvs */
            };
        };
        byte_t               *data;        /* callback data */
        size_t               size;         /* message size  */
        int                  remote;       /* remote peer   */
        lci_cb_handle_type_t type;         /* callback type */
    } args;                                                      /* 56 bytes */
    alignas(8) union /* callback function */ {
        parsec_ce_am_callback_t       am;
        parsec_ce_onesided_callback_t put_origin;
        parsec_ce_am_callback_t       put_target;
        parsec_ce_onesided_callback_t get_origin;
        parsec_ce_am_callback_t       get_target;
    } cb;                                                        /*  8 bytes */
} lci_cb_handle_t;
static OBJ_CLASS_INSTANCE(lci_cb_handle_t, parsec_list_item_t, NULL, NULL);

/* memory pool for callbacks */
static parsec_mempool_t lci_cb_handle_mempool;

/* hash table for AM callbacks */
static parsec_hash_table_t am_cb_hash_table;

static parsec_key_fn_t key_fns = {
    .key_equal = parsec_hash_table_generic_64bits_key_equal,
    .key_print = parsec_hash_table_generic_64bits_key_print,
    .key_hash  = parsec_hash_table_generic_64bits_key_hash
};

/* LCI request handle (for pool) */
typedef struct lci_req_handle_s {
    alignas(128) parsec_list_item_t      list_item; /* for mempool */
    alignas(8)   parsec_thread_mempool_t *mempool_owner;
    alignas(64)  lc_req                  req;
} lci_req_handle_t;
static OBJ_CLASS_INSTANCE(lci_req_handle_t, parsec_list_item_t, NULL, NULL);

/* memory pool for requests */
static parsec_mempool_t lci_req_mempool;

/* LCI dynamic message (for pool) */
typedef struct lci_dynmsg_s {
    alignas(128) parsec_list_item_t      list_item;
    alignas(8)   parsec_thread_mempool_t *mempool_owner;
    alignas(64)  byte_t                  data[]; /* cache-line alignment */
} lci_dynmsg_t;
static OBJ_CLASS_INSTANCE(lci_dynmsg_t, parsec_list_item_t, NULL, NULL);

/* memory pool for dynamic messages */
static parsec_mempool_t lci_dynmsg_mempool;

static parsec_list_t lci_progress_cb_fifo;
static parsec_list_t lci_cb_fifo;

/* LCI one-sided activate message handshake type */
typedef struct lci_handshake_s {
    byte_t            *buffer;
    size_t            size;
    parsec_ce_tag_t   cb;
    alignas(8) byte_t cb_data[];
} lci_handshake_t;

static atomic_int current_tag = 0;

/* global endpoint */
lc_ep *lci_global_ep = NULL;
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
static pthread_cond_t  progress_condition = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t progress_mutex     = PTHREAD_MUTEX_INITIALIZER;
static enum { RUN, PAUSE, STOP } progress_status = RUN;
#endif
static atomic_bool progress_thread_stop = false;
static pthread_t progress_thread_id;

static inline void * dyn_alloc(size_t size, void **ctx)
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

/* just push onto cb queue */
static inline void lci_put_send_cb(void *ctx)
{
    /* ctx is pointer to lci_cb_handle_t */
    lci_cb_handle_t *handle = ctx;
    lci_ce_debug_verbose("Put Origin end:\t%d(%p+%td) -> %d(%p+%td) size %zu",
                         ep_rank,
                         (void *)((lci_mem_reg_handle_t *)handle->args.lreg)->mem,
                         handle->args.ldispl,
                         handle->args.remote,
                         (void *)((lci_mem_reg_handle_t *)handle->args.rreg)->mem,
                         handle->args.rdispl,
                         handle->args.size);
    if (pthread_equal(progress_thread_id, pthread_self())) {
        /* we are on progress thread, push to back of progress cb fifo */
        parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
    } else {
        /* we aren't on progress thread, just call the callback */
        lci_ce_debug_verbose("Put Origin cb:\t%d(%p+%td) -> %d(%p+%td) size %zu data %p",
                             ep_rank,
                             (void *)((lci_mem_reg_handle_t *)handle->args.lreg)->mem,
                             handle->args.ldispl,
                             handle->args.remote,
                             (void *)((lci_mem_reg_handle_t *)handle->args.rreg)->mem,
                             handle->args.rdispl,
                             handle->args.size, handle->args.data);
        handle->cb.put_origin(&parsec_ce,
                              handle->args.lreg, handle->args.ldispl,
                              handle->args.rreg, handle->args.rdispl,
                              handle->args.size, handle->args.remote,
                              handle->args.data);
        /* return handle to mempool */
        parsec_mempool_free(&lci_cb_handle_mempool, handle);
    }
}

/* just push onto cb queue */
static inline void lci_get_send_cb(void *ctx)
{
    /* ctx is pointer to lci_cb_handle_t */
    lci_cb_handle_t *handle = ctx;
    lci_ce_debug_verbose("Get Target end:\t%d <- %d(%p) size %zu with tag %d",
                         handle->args.remote, ep_rank,
                         (void *)handle->args.msg, handle->args.size,
                         handle->args.tag);
    if (pthread_equal(progress_thread_id, pthread_self())) {
        /* we are on progress thread, push to back of progress cb fifo */
        parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
    } else {
        lci_handshake_t *handshake = NULL;
        lci_dynmsg_t    *dynmsg    = NULL;
        /* we aren't on progress thread, just call the callback */
        lci_ce_debug_verbose("Get Target cb:\t%d <- %d(%p) size %zu with tag %d",
                             handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);
#if 0
        handle->cb.get_target(&parsec_ce,
                              handle->args.tag,  handle->args.msg,
                              handle->args.size, handle->args.remote,
                              handle->args.data);
#endif
        /* API seems to be bugged, pass callback data as message */
        handle->cb.get_target(&parsec_ce,
                              handle->args.tag,  handle->args.data,
                              handle->args.size, handle->args.remote,
                              NULL);
        /* return memory from AM */
        /* handle->args.data points to the cb_data field of the handshake info
         * which is the data field of the dynmsg object */
        handshake = container_of(handle->args.data, lci_handshake_t, cb_data);
        dynmsg    = container_of(handshake, lci_dynmsg_t, data);
        parsec_mempool_free(&lci_dynmsg_mempool, dynmsg);
        /* return handle to mempool */
        parsec_mempool_free(&lci_cb_handle_mempool, handle);
    }
}

void * lci_progress_thread(void *arg)
{
    parsec_list_t deferred_fifo;
    parsec_list_item_t *ring = NULL;
    lc_req *req = NULL;

    lci_ce_debug_verbose("progress thread start");

    /* bind thread */
    parsec_context_t *context = arg;
    remote_dep_bind_thread(context);

    OBJ_CONSTRUCT(&deferred_fifo, parsec_list_t);

    /* loop until told to stop */
    while (!atomic_load_explicit(&progress_thread_stop, memory_order_acquire)) {
        /* progress until nothing progresses */
        while (lc_progress(0))
            continue;

        /* start deferred sends and receives */
        // this could maybe be done on comm thread
        for (parsec_list_item_t *item = parsec_list_nolock_pop_front(&deferred_fifo);
                                 item != NULL;
                                 item = parsec_list_nolock_pop_front(&deferred_fifo)) {
            lci_cb_handle_t *handle = container_of(item, lci_cb_handle_t, list_item);
            switch (handle->args.type) {
            case LCI_PUT_TARGET:
                lci_ce_debug_verbose("Put Target start:\t%d -> %d(%p) size %zu with tag %d",
                                     handle->args.remote, ep_rank,
                                     (void *)handle->args.msg, handle->args.size,
                                     handle->args.tag);
                if (LC_OK == lc_recv(handle->args.msg, handle->args.size,
                                     handle->args.remote, handle->args.tag,
                                     put_ep, handle->args.req)) {
                    /* receive start was successful, go on to next iteration */
                    continue;
                } else {
                    lci_ce_debug_verbose("Put Target defer:\t%d -> %d(%p) size %zu with tag %d",
                                         handle->args.remote, ep_rank,
                                         (void *)handle->args.msg, handle->args.size,
                                         handle->args.tag);
                }
                break;
            case LCI_GET_TARGET:
                lci_ce_debug_verbose("Get Target start:\t%d <- %d(%p) size %zu with tag %d",
                                     handle->args.remote, ep_rank,
                                     (void *)handle->args.msg,
                                     handle->args.size, handle->args.tag);
                if (LC_OK == lc_send(handle->args.msg, handle->args.size,
                                     handle->args.remote, handle->args.tag,
                                     get_ep, lci_get_send_cb, handle)) {
                    /* send start was successful, go on to next iteration */
                    continue;
                } else {
                    lci_ce_debug_verbose("Get Target defer:\t%d <- %d(%p) size %zu with tag %d",
                                         handle->args.remote, ep_rank,
                                         (void *)handle->args.msg,
                                         handle->args.size, handle->args.tag);
                }
                break;
            default:
                lci_ce_fatal("invalid callback type: %d", handle->args.type);
                break;
            }
            /* if we got here, send/receive didn't start
             * push request to front of deferred fifo and bail out of loop */
            parsec_list_nolock_push_front(&deferred_fifo, &handle->list_item);
            break;
        }

        /* handle abort */
        if (LC_OK == lc_cq_pop(abort_ep, &req)) {
            lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
            handle->args.tag    = req->meta;
            handle->args.remote = req->rank;
            handle->args.type   = LCI_ABORT;
            lc_cq_reqfree(abort_ep, req);
            lci_ce_debug_verbose("Abort %d recv:\t%d -> %d",
                                 (int)handle->args.tag,
                                 handle->args.remote, ep_rank);
            /* push to front of local callback fifo */
            parsec_list_nolock_push_front(&lci_progress_cb_fifo, &handle->list_item);
        }

        /* handle active messages - at target */
        while (LC_OK == lc_cq_pop(am_ep, &req)) {
            parsec_key_t key = req->meta;
            lci_ce_debug_verbose("Active Message %"PRIu64" recv:\t%d -> %d message %p size %zu",
                                 key, req->rank, ep_rank, req->buffer, req->size);
            /* find AM handle, based on active message tag */
            lci_cb_handle_t *am_handle = parsec_hash_table_find(&am_cb_hash_table, key);
            /* warn if not found */
            if (NULL == am_handle) {
                parsec_warning("LCI[%d]:\tActive Message %"PRIu64" not registered",
                        ep_rank, key);
                lc_cq_reqfree(am_ep, req);
                continue;
            }

            /* allocate callback handle & fill with info */
            lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
            handle->args.tag    = am_handle->args.tag;
            handle->args.msg    = req->buffer;
            handle->args.data   = am_handle->args.data;
            handle->args.size   = req->size;
            handle->args.remote = req->rank;
            handle->args.type   = LCI_ACTIVE_MESSAGE;
            handle->cb.am       = am_handle->cb.am;

            lc_cq_reqfree(am_ep, req);
            /* push to local callback fifo */
            parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
        }

        /* repond to put request - at target */
        while (LC_OK == lc_cq_pop(put_am_ep, &req)) {
            lci_handshake_t *handshake = req->buffer;

            lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
            handle->args.tag         = req->meta;
            handle->args.msg         = handshake->buffer;
            /* array to pointer decay for handshake->cb_data */
            handle->args.data        = handshake->cb_data;
            handle->args.size        = handshake->size;
            handle->args.remote      = req->rank;
            handle->args.type        = LCI_PUT_TARGET;
            handle->cb.put_target    = (parsec_ce_am_callback_t)handshake->cb;

            lci_ce_debug_verbose("Put Target handshake:\t%d -> %d(%p) size %zu with tag %d, cb data %p",
                                 handle->args.remote, ep_rank,
                                 (void *)handle->args.msg, handle->args.size,
                                 handle->args.tag, (void *)handle->args.data);
            lc_cq_reqfree(put_am_ep, req);

            /* get request from pool and set context to callback handle */
            lci_req_handle_t *req_handle = parsec_thread_mempool_allocate(
                                              lci_req_mempool.thread_mempools);
            lc_req *recv_req = &req_handle->req;
            recv_req->ctx = handle;

            /* start receive for the put */
            lci_ce_debug_verbose("Put Target start:\t%d -> %d(%p) size %zu with tag %d",
                                 handle->args.remote, ep_rank,
                                 (void *)handle->args.msg, handle->args.size,
                                 handle->args.tag);
            if (LC_OK != lc_recv(handle->args.msg, handle->args.size,
                                 handle->args.remote, handle->args.tag,
                                 put_ep, recv_req)) {
                lci_ce_debug_verbose("Put Target defer:\t%d -> %d(%p) size %zu with tag %d",
                                     handle->args.remote, ep_rank,
                                     (void *)handle->args.msg, handle->args.size,
                                     handle->args.tag);
                /* save allocated request, so we don't need to reallocate */
                handle->args.req = recv_req;
                /* if receive didn't start, push request to deferred fifo */
                parsec_list_nolock_push_back(&deferred_fifo, &handle->list_item);
            }
        }

        /* put receive - at target */
        while (LC_OK == lc_cq_pop(put_ep, &req)) {
            /* get callback handle from request context */
            lci_cb_handle_t *handle = req->ctx;
            lci_ce_debug_verbose("Put Target end:\t%d -> %d(%p) size %zu with tag %d",
                                 handle->args.remote, ep_rank,
                                 (void *)handle->args.msg, handle->args.size,
                                 handle->args.tag);
            /* return request to pool */
            lc_cq_reqfree(put_ep, req); // returns packet to pool, not req
            lci_req_handle_t *req_handle = container_of(req, lci_req_handle_t, req);
            parsec_mempool_free(&lci_req_mempool, req_handle);
            /* push to local callback fifo */
            parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
        }

        /* respond to get request - at target */
        while (LC_OK == lc_cq_pop(get_am_ep, &req)) {
            lci_handshake_t *handshake = req->buffer;

            lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
            handle->args.tag         = req->meta;
            handle->args.msg         = handshake->buffer;
            /* array to pointer decay for handshake->cb_data */
            handle->args.data        = handshake->cb_data;
            handle->args.size        = handshake->size;
            handle->args.remote      = req->rank;
            handle->args.type        = LCI_GET_TARGET;
            handle->cb.get_target    = (parsec_ce_am_callback_t)handshake->cb;

            lci_ce_debug_verbose("Get Target handshake:\t%d <- %d(%p) size %zu with tag %d, cb data %p",
                                 handle->args.remote, ep_rank,
                                 (void *)handle->args.msg, handle->args.size,
                                 handle->args.tag, (void *)handle->args.data);
            lc_cq_reqfree(get_am_ep, req);

            /* start send for the get */
            lci_ce_debug_verbose("Get Target start:\t%d <- %d(%p) size %zu with tag %d",
                                 handle->args.remote, ep_rank,
                                 (void *)handle->args.msg, handle->args.size,
                                 handle->args.tag);
            if (LC_OK != lc_send(handle->args.msg, handle->args.size,
                                 handle->args.remote, handle->args.tag,
                                 get_ep, lci_get_send_cb, handle)) {
                /* send didn't start, push request to deferred fifo */
                lci_ce_debug_verbose("Get Target defer:\t%d <- %d(%p) size %zu with tag %d",
                                     handle->args.remote, ep_rank,
                                     (void *)handle->args.msg,
                                     handle->args.size, handle->args.tag);
                parsec_list_nolock_push_back(&deferred_fifo, &handle->list_item);
            }
        }

        /* get receive - at origin */
        while (LC_OK == lc_cq_pop(get_ep, &req)) {
            /* get callback handle from request context */
            lci_cb_handle_t *handle = req->ctx;
            lci_ce_debug_verbose("Get Origin end:\t%d(%p) <- %d(%p) size %zu with tag %d",
                                 ep_rank,
                                 (void *)((lci_mem_reg_handle_t *)handle->args.lreg)->mem,
                                 handle->args.ldispl,
                                 handle->args.remote,
                                 (void *)((lci_mem_reg_handle_t *)handle->args.rreg)->mem,
                                 handle->args.rdispl,
                                 handle->args.size, req->meta);
            /* return request to pool */
            lc_cq_reqfree(get_ep, req); // returns packet to pool, not req
            lci_req_handle_t *req_handle = container_of(req, lci_req_handle_t, req);
            parsec_mempool_free(&lci_req_mempool, req_handle);
            /* push to local callback fifo */
            parsec_list_nolock_push_back(&lci_progress_cb_fifo, &handle->list_item);
        }

        /* push back to shared callback fifo */
        if (NULL != (ring = parsec_list_nolock_unchain(&lci_progress_cb_fifo))) {
            parsec_list_chain_back(&lci_cb_fifo, ring);
        }
#if 0
        /* push back to shared callback fifo if we could get the lock
         * if we can't, we'll try again the next iteration */
        if (!parsec_list_nolock_is_empty(&lci_progress_cb_fifo) &&
             parsec_atomic_trylock(&lci_cb_fifo.atomic_lock)) {
            ring = parsec_list_nolock_unchain(&lci_progress_cb_fifo);
            parsec_list_nolock_chain_back(&lci_cb_fifo, ring);
            parsec_atomic_unlock(&lci_cb_fifo.atomic_lock);
        }
#endif

        /* sleep for comm_yield_ns if set to comm_yield, else yield thread  */
        const struct timespec ts = { .tv_sec = 0, .tv_nsec = comm_yield_ns };
        switch (comm_yield) {
        case 1:
        case 2:
            nanosleep(&ts, NULL);
            break;
        default:
            sched_yield();
            break;
        }
    }

#if 0
    _Bool stop = false;
    while (!stop) {
        pthread_mutex_lock(&progress_mutex);
        switch (progress_status) {
        case RUN:
            lc_progress(0);
            break;
        case PAUSE:
            pthread_cond_wait(&progress_condition, &progress_mutex);
            break;
        case STOP:
            stop = true;
            break;
        }
        pthread_mutex_unlock(&progress_mutex);
    }
#endif

    OBJ_DESTRUCT(&deferred_fifo);
    lci_ce_debug_verbose("progress thread stop");
    return NULL;
}

/* Initialize LCI communication engine */
parsec_comm_engine_t *
lci_init(parsec_context_t *context)
{
    /* set size and rank */
    lc_get_num_proc(&ep_size);
    lc_get_proc_num(&ep_rank);
    if (NULL != context) {
        context->nb_nodes = ep_size;
        context->my_rank  = ep_rank;
    }

    lci_ce_debug_verbose("init");

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
    parsec_ce.send_active_message = lci_send_active_message;
    parsec_ce.parsec_context = context;
    parsec_ce.capabilites.sided = 1;
    parsec_ce.capabilites.supports_noncontiguous_datatype = 0;

    /* create a mempool for memory registration */
    parsec_mempool_construct(&lci_mem_reg_handle_mempool,
                             OBJ_CLASS(lci_mem_reg_handle_t),
                             sizeof(lci_mem_reg_handle_t),
                             offsetof(lci_mem_reg_handle_t, mempool_owner),
                             1);
    /* set alignment to 2^6 = 64 bytes */
    for (size_t i = 0; i < lci_mem_reg_handle_mempool.nb_thread_mempools; i++) {
        lci_mem_reg_handle_mempool.thread_mempools[i].mempool.alignment = 6;
    }

    /* create a mempool for callbacks */
    parsec_mempool_construct(&lci_cb_handle_mempool,
                             OBJ_CLASS(lci_cb_handle_t),
                             sizeof(lci_cb_handle_t),
                             offsetof(lci_cb_handle_t, mempool_owner),
                             1);
    /* set alignment to 2^7 = 128 bytes */
    for (size_t i = 0; i < lci_cb_handle_mempool.nb_thread_mempools; i++) {
        lci_cb_handle_mempool.thread_mempools[i].mempool.alignment = 7;
    }

    /* create a mempool for requests */
    parsec_mempool_construct(&lci_req_mempool,
                             OBJ_CLASS(lci_req_handle_t),
                             sizeof(lci_req_handle_t),
                             offsetof(lci_req_handle_t, mempool_owner),
                             1);
    /* set alignment to 2^7 = 128 bytes */
    for (size_t i = 0; i < lci_req_mempool.nb_thread_mempools; i++) {
        lci_req_mempool.thread_mempools[i].mempool.alignment = 7;
    }

    /* create a mempool for dynamic messages */
    parsec_mempool_construct(&lci_dynmsg_mempool,
                             OBJ_CLASS(lci_dynmsg_t),
                             /* ensure enough space for any medium message */
                             sizeof(lci_dynmsg_t) + lc_max_medium(0),
                             offsetof(lci_dynmsg_t, mempool_owner),
                             1);
    /* set alignment to 2^7 = 128 bytes */
    for (size_t i = 0; i < lci_dynmsg_mempool.nb_thread_mempools; i++) {
        lci_dynmsg_mempool.thread_mempools[i].mempool.alignment = 7;
    }

    /* create send callback queues */
    OBJ_CONSTRUCT(&lci_progress_cb_fifo, parsec_list_t);
    OBJ_CONSTRUCT(&lci_cb_fifo, parsec_list_t);

    /* allocated hash tables for callbacks */
    OBJ_CONSTRUCT(&am_cb_hash_table, parsec_hash_table_t);
    parsec_hash_table_init(&am_cb_hash_table,
                           offsetof(lci_cb_handle_t, ht_item),
                           4, key_fns, &am_cb_hash_table);

    /* init LCI */
    lc_ep *default_ep = context->comm_ctx;
    lc_opt opt = { .dev = 0 };

    /* collective endpoint */
    opt.desc = LC_EXP_SYNC;
    lc_ep_dup(&opt, *default_ep, &collective_ep);

    /* active message endpoint */
    opt.desc = LC_DYN_CQ;
    opt.alloc = dyn_alloc;
    lc_ep_dup(&opt, *default_ep, &am_ep);

    /* one-sided AM endpoint */
    lc_ep_dup(&opt, *default_ep, &put_am_ep);
    lc_ep_dup(&opt, *default_ep, &get_am_ep);

    /* abort endpoint */
    lc_ep_dup(&opt, *default_ep, &abort_ep);

    /* one-sided endpoint */
    opt.desc = LC_EXP_CQ;
    opt.alloc = NULL;
    lc_ep_dup(&opt, *default_ep, &put_ep);
    lc_ep_dup(&opt, *default_ep, &get_ep);

    /* start progress thread */
    lci_ce_debug_verbose("starting progress thread");
    atomic_store_explicit(&progress_thread_stop, false, memory_order_release);
    pthread_create(&progress_thread_id, NULL, lci_progress_thread, context);

    return &parsec_ce;
}

/* Finalize LCI communication engine */
int
lci_fini(parsec_comm_engine_t *comm_engine)
{
    lci_ce_debug_verbose("fini");
    lci_sync(comm_engine);
    void *progress_retval = NULL;

    /* stop progress thread */
    lci_ce_debug_verbose("stopping progress thread");
    atomic_store_explicit(&progress_thread_stop, true, memory_order_release);
#if 0
    pthread_mutex_lock(&progress_mutex);
    progress_status = STOP;
    pthread_cond_signal(&progress_condition); /* if paused */
    pthread_mutex_unlock(&progress_mutex);
#endif
    pthread_join(progress_thread_id, &progress_retval);

    OBJ_DESTRUCT(&lci_cb_fifo);
    OBJ_DESTRUCT(&lci_progress_cb_fifo);

    parsec_hash_table_fini(&am_cb_hash_table);
    OBJ_DESTRUCT(&am_cb_hash_table);

    parsec_mempool_destruct(&lci_dynmsg_mempool);
    parsec_mempool_destruct(&lci_req_mempool);
    parsec_mempool_destruct(&lci_cb_handle_mempool);
    parsec_mempool_destruct(&lci_mem_reg_handle_mempool);

    return 1;
}

int lci_tag_register(parsec_ce_tag_t tag,
                     parsec_ce_am_callback_t cb,
                     void *cb_data,
                     size_t msg_length)
{
    parsec_key_t key = tag;
    /* allocate from mempool */
    lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
    handle->ht_item.key = key;
    handle->args.tag    = tag;
    handle->args.data   = cb_data;
    handle->args.type   = LCI_ACTIVE_MESSAGE;
    handle->cb.am       = cb;

    lci_ce_debug_verbose("register Active Message %"PRIu64" data %p size %zu",
                         tag, cb_data, msg_length);
    parsec_hash_table_lock_bucket(&am_cb_hash_table, key);
    if (NULL != parsec_hash_table_nolock_find(&am_cb_hash_table, key)) {
        parsec_warning("LCI[%d]:\tActive Message %"PRIu64" already registered",
                       ep_rank, tag);
        parsec_hash_table_unlock_bucket(&am_cb_hash_table, key);
        return 0;
    }

    parsec_hash_table_nolock_insert(&am_cb_hash_table, &handle->ht_item);
    parsec_hash_table_unlock_bucket(&am_cb_hash_table, key);
    return 1;
}

int lci_tag_unregister(parsec_ce_tag_t tag)
{
    lci_ce_debug_verbose("unregister Active Message %"PRIu64, tag);
    parsec_key_t key = tag;
    lci_cb_handle_t *handle = parsec_hash_table_remove(&am_cb_hash_table, key);
    if (NULL == handle) {
        parsec_warning("LCI[%d]:\tActive Message %"PRIu64" not registered", ep_rank, tag);
        return 0;
    }
    parsec_mempool_free(&lci_cb_handle_mempool, handle);
    return 1;
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

    lci_ce_debug_verbose("register memory %p size %zu", mem, mem_size);

    /* allocate from mempool */
    lci_mem_reg_handle_t *handle = parsec_thread_mempool_allocate(
                                 lci_mem_reg_handle_mempool.thread_mempools);
    /* set mem handle info */
    handle->mem      = mem;
    handle->size     = mem_size;
    handle->count    = count;
    handle->datatype = datatype;

    /* register memory with LCI for put/get */
    //LCI_register(mem, mem_size);

    /* return the pointer to the handle and its size */
    *lreg = (parsec_ce_mem_reg_handle_t)handle;
    *lreg_size = sizeof(*handle);
    return 1;
}

int
lci_mem_unregister(parsec_ce_mem_reg_handle_t *lreg)
{
    lci_mem_reg_handle_t *handle = (lci_mem_reg_handle_t *) *lreg;
    lci_ce_debug_verbose("unregister memory %p size %zu",
                         (void *)handle->mem, handle->size);
    //LCI_unregister(handle->mem);
    parsec_mempool_free(&lci_mem_reg_handle_mempool, handle);
    *lreg = NULL;
    return 1;
}

/* Size of an LCI memory handle */
int
lci_get_mem_reg_handle_size(void)
{
    return sizeof(lci_mem_reg_handle_t);
}

int
lci_mem_retrieve(parsec_ce_mem_reg_handle_t lreg,
                 void **mem, parsec_datatype_t *datatype, int *count)
{
    lci_mem_reg_handle_t *handle = (lci_mem_reg_handle_t *)lreg;
    *mem      = handle->mem;
    *count    = handle->count;
    *datatype = handle->datatype;
    lci_ce_debug_verbose("retrieve memory %p size %zu",
                         (void *)handle->mem, handle->size);
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
    size_t buffer_size = sizeof(lci_handshake_t) + r_cb_data_size;
    assert(buffer_size <= lc_max_medium(0) && "active message data too long");
    alignas(lci_handshake_t) byte_t buffer[buffer_size];
    lci_handshake_t *handshake = (lci_handshake_t *)&buffer;

    /* get next tag */
    int tag = atomic_fetch_add_explicit(&current_tag, 1, memory_order_relaxed);

    lci_mem_reg_handle_t *ldata = (lci_mem_reg_handle_t *)lreg;
    lci_mem_reg_handle_t *rdata = (lci_mem_reg_handle_t *)rreg;

    void *lbuf = ldata->mem + ldispl;
    void *rbuf = rdata->mem + rdispl;

    // NOTE: size is always passed as 0, use ldata->size and rdata->size

    /* set handshake info */
    handshake->buffer = rbuf;
    handshake->size   = rdata->size;
    handshake->cb     = r_tag;
    memcpy(handshake->cb_data, r_cb_data, r_cb_data_size);

    /* send handshake to remote, will be retrieved from queue */
    lci_ce_debug_verbose("Put Origin handshake:\t%d(%p+%td) -> %d(%p+%td) size %zu with tag %d",
                         ep_rank, (void *)ldata->mem, ldispl,
                         remote,  (void *)rdata->mem, rdispl,
                         ldata->size, tag);
    RETRY(lc_sendm(handshake, buffer_size, remote, tag, put_am_ep));

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

    /* start send to remote with tag */
    lci_ce_debug_verbose("Put Origin start:\t%d(%p+%td) -> %d(%p+%td) size %zu with tag %d",
                         ep_rank, (void *)ldata->mem, ldispl,
                         remote,  (void *)rdata->mem, rdispl,
                         ldata->size, tag);
    RETRY(lc_send(lbuf, ldata->size, remote, tag, put_ep, lci_put_send_cb, handle));
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
    size_t buffer_size = sizeof(lci_handshake_t) + r_cb_data_size;
    assert(buffer_size <= lc_max_medium(0) && "active message data too long");
    byte_t buffer[buffer_size];
    lci_handshake_t *handshake = (lci_handshake_t *)&buffer;

    /* get next tag */
    int tag = atomic_fetch_add_explicit(&current_tag, 1, memory_order_relaxed);

    lci_mem_reg_handle_t *ldata = (lci_mem_reg_handle_t *)lreg;
    lci_mem_reg_handle_t *rdata = (lci_mem_reg_handle_t *)rreg;

    void *lbuf = ldata->mem + ldispl;
    void *rbuf = rdata->mem + rdispl;

    // NOTE: size is always passed as 0, use ldata->size and rdata->size

    /* set handshake info */
    handshake->buffer = rbuf;
    handshake->size   = rdata->size;
    handshake->cb     = r_tag;
    memcpy(handshake->cb_data, r_cb_data, r_cb_data_size);

    /* send handshake to remote, will be retrieved from queue */
    lci_ce_debug_verbose("Get Origin handshake:\t%d(%p+%td) <- %d(%p+%td) size %zu with tag %d",
                         ep_rank, (void *)ldata->mem, ldispl,
                         remote,  (void *)rdata->mem, rdispl,
                         ldata->size, tag);
    RETRY(lc_sendm(handshake, buffer_size, remote, tag, get_am_ep));

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

    /* get request from pool and set context to callback handle */
    lci_req_handle_t *req_handle = parsec_thread_mempool_allocate(
                                              lci_req_mempool.thread_mempools);
    lc_req *req = &req_handle->req;
    req->ctx = handle;

    /* start recieve from remote with tag */
    lci_ce_debug_verbose("Get Origin start:\t%d(%p+%td) <- %d(%p+%td) size %zu with tag %d",
                         ep_rank, (void *)ldata->mem, ldispl,
                         remote,  (void *)rdata->mem, rdispl,
                         ldata->size, tag);
    RETRY(lc_recv(lbuf, ldata->size, remote, tag, get_ep, req));
    return 1;
}

int
lci_send_active_message(parsec_comm_engine_t *comm_engine,
                        parsec_ce_tag_t tag,
                        int remote,
                        void *addr, size_t size)
{
    assert(size <= lc_max_medium(0) && "active message data too long");
    lci_ce_debug_verbose("Active Message %"PRIu64" send:\t%d -> %d with message %p size %zu",
                         tag, ep_rank, remote, addr, size);
    RETRY(lc_sendm(addr, size, remote, tag, am_ep));
    return 1;
}

_Noreturn void
lci_abort(int exit_code)
{
    lci_ce_debug_verbose("Abort %d", exit_code);
    for (int i = 0; i < ep_size; i++) {
        if (i != ep_rank) {
            /* send abort message to all other processes */
            lci_ce_debug_verbose("Abort %d send:\t%d -> %d", exit_code, ep_rank, i);
            RETRY(lc_sends(NULL, 0, i, exit_code, abort_ep));
        }
    }
    lci_ce_debug_verbose("Abort %d barrier:\t%d ->", exit_code, ep_rank);
    /* wait for all processes to ack the abort */
    lc_barrier(collective_ep);
    /* exit without cleaning up */
    _Exit(exit_code);
}

int
lci_cb_progress(parsec_comm_engine_t *comm_engine)
{
    int ret = 0;
    parsec_list_t cb_fifo;
    parsec_list_item_t *ring = NULL;

    OBJ_CONSTRUCT(&cb_fifo, parsec_list_t);
    if (NULL != (ring = parsec_list_unchain(&lci_cb_fifo))) {
        parsec_list_nolock_chain_back(&cb_fifo, ring);
    }

    for (parsec_list_item_t *item = parsec_list_nolock_pop_front(&cb_fifo);
                             item != NULL;
                             item = parsec_list_nolock_pop_front(&cb_fifo)) {
        lci_cb_handle_t *handle = container_of(item, lci_cb_handle_t, list_item);
        lci_handshake_t *handshake = NULL;
        lci_dynmsg_t    *dynmsg    = NULL;

        switch (handle->args.type) {
        case LCI_ABORT:
            lci_ce_debug_verbose("Abort %d barrier:\t%d ->",
                                 (int)handle->args.tag, handle->args.remote);
            /* wait for all processes to ack the abort */
            lc_barrier(collective_ep);
            /* exit without cleaning up */
            _Exit(handle->args.tag);
            break;

        case LCI_ACTIVE_MESSAGE:
            lci_ce_debug_verbose("Active Message %"PRIu64" cb:\t%d -> %d message %p size %zu",
                                 handle->args.tag, handle->args.remote, ep_rank,
                                 (void *)handle->args.msg, handle->args.size);
            handle->cb.am(comm_engine, handle->args.tag,
                          handle->args.msg, handle->args.size,
                          handle->args.remote, handle->args.data);
            /* return memory to pool (only allocated if size > 0) */
            if (handle->args.size > 0) {
                dynmsg = container_of(handle->args.msg, lci_dynmsg_t, data);
                parsec_mempool_free(&lci_dynmsg_mempool, dynmsg);
            }
            break;

        case LCI_PUT_ORIGIN:
            lci_ce_debug_verbose("Put Origin cb:\t%d(%p+%td) -> %d(%p+%td) size %zu data %p",
                                 ep_rank,
                                 (void *)((lci_mem_reg_handle_t *)handle->args.lreg)->mem,
                                 handle->args.ldispl,
                                 handle->args.remote,
                                 (void *)((lci_mem_reg_handle_t *)handle->args.rreg)->mem,
                                 handle->args.rdispl,
                                 handle->args.size, handle->args.data);
            handle->cb.put_origin(comm_engine,
                                  handle->args.lreg, handle->args.ldispl,
                                  handle->args.rreg, handle->args.rdispl,
                                  handle->args.size, handle->args.remote,
                                  handle->args.data);
            break;

        case LCI_PUT_TARGET:
            lci_ce_debug_verbose("Put Target cb:\t%d -> %d(%p) size %zu with tag %d",
                                 handle->args.remote, ep_rank,
                                 (void *)handle->args.msg, handle->args.size,
                                 handle->args.tag);
#if 0
            handle->cb.put_target(comm_engine,
                                  handle->args.tag,  handle->args.msg,
                                  handle->args.size, handle->args.remote,
                                  handle->args.data);
#endif
            /* API seems to be bugged, pass callback data as message */
            handle->cb.put_target(comm_engine,
                                  handle->args.tag,  handle->args.data,
                                  handle->args.size, handle->args.remote,
                                  NULL);
            /* return memory from AM */
            /* handle->args.data points to the cb_data field of the handshake info
             * which is the data field of the dynmsg object */
            handshake = container_of(handle->args.data, lci_handshake_t, cb_data);
            dynmsg    = container_of(handshake, lci_dynmsg_t, data);
            parsec_mempool_free(&lci_dynmsg_mempool, dynmsg);
            break;

        case LCI_GET_ORIGIN:
            lci_ce_debug_verbose("Get Origin cb:\t%d(%p+%td) <- %d(%p+%td) size %zu data %p",
                                 ep_rank,
                                 (void *)((lci_mem_reg_handle_t *)handle->args.lreg)->mem,
                                 handle->args.ldispl,
                                 handle->args.remote,
                                 (void *)((lci_mem_reg_handle_t *)handle->args.rreg)->mem,
                                 handle->args.rdispl,
                                 handle->args.size, handle->args.data);
            handle->cb.get_origin(comm_engine,
                                  handle->args.lreg, handle->args.ldispl,
                                  handle->args.rreg, handle->args.rdispl,
                                  handle->args.size, handle->args.remote,
                                  handle->args.data);
            break;

        case LCI_GET_TARGET:
            lci_ce_debug_verbose("Get Target cb:\t%d <- %d(%p) size %zu with tag %d",
                                 handle->args.remote, ep_rank,
                                 (void *)handle->args.msg, handle->args.size,
                                 handle->args.tag);
#if 0
            handle->cb.get_target(comm_engine,
                                  handle->args.tag,  handle->args.msg,
                                  handle->args.size, handle->args.remote,
                                  handle->args.data);
#endif
            /* API seems to be bugged, pass callback data as message */
            handle->cb.get_target(comm_engine,
                                  handle->args.tag,  handle->args.data,
                                  handle->args.size, handle->args.remote,
                                  NULL);
            /* return memory from AM */
            /* handle->args.data points to the cb_data field of the handshake info
             * which is the data field of the dynmsg object */
            handshake = container_of(handle->args.data, lci_handshake_t, cb_data);
            dynmsg    = container_of(handshake, lci_dynmsg_t, data);
            parsec_mempool_free(&lci_dynmsg_mempool, dynmsg);
            break;
        default:
            lci_ce_fatal("invalid callback type: %d", handle->args.type);
            break;
        }

        /* return handle to mempool */
        parsec_mempool_free(&lci_cb_handle_mempool, handle);
        ret++;
    }

    OBJ_DESTRUCT(&cb_fifo);
}

#if 0
int
lci_progress(parsec_comm_engine_t *comm_engine)
{
    int ret = 0;
    lc_req *req;

    /* handle abort */
    if (LC_OK == lc_cq_pop(abort_ep, &req)) {
        lci_ce_debug_verbose("Abort %d recv:\t%d -> %d",
                             req->meta, req->rank, ep_rank);
        int exit_code = req->meta;
        lc_cq_reqfree(abort_ep, req);
        /* wait for all processes to ack the abort */
        lc_barrier(collective_ep);
        /* exit without cleaning up */
        _Exit(exit_code);
    }

    /* handle active messages */
    while (LC_OK == lc_cq_pop(am_ep, &req)) {
        parsec_key_t key = req->meta;
        /* find callback handle, based on active message tag */
        lci_cb_handle_t *handle = parsec_hash_table_nolock_find(&am_cb_hash_table, key);
        lci_ce_debug_verbose("Active Message %"PRIu64" recv:\t%d -> %d with message %p size %zu",
                             key, req->rank, ep_rank, req->buffer, req->size);
        /* if callback found, call it; else warn */
        if (NULL != handle) {
            handle->cb.am(comm_engine, handle->args.tag,
                          req->buffer, req->size,
                          req->rank, handle->args.data);
        } else {
            parsec_warning("LCI[%d]:\tActive Message %"PRIu64" not registered",
                           ep_rank, key);
        }
        /* return memory to pool (only allocated if size > 0) */
        if (req->size > 0) {
            lci_dynmsg_t *dynmsg = container_of(req->buffer, lci_dynmsg_t, data);
            parsec_thread_mempool_free(dynmsg->mempool_owner, dynmsg);
        }
        lc_cq_reqfree(am_ep, req);
        ret++;
    }

    /* repond to put request */
    while (LC_OK == lc_cq_pop(put_am_ep, &req)) {
        lci_handshake_t *handshake = req->buffer;

        lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
        handle->cb.put_target    = (parsec_ce_am_callback_t)handshake->cb;
        handle->args.tag         = req->meta;
        handle->args.msg         = handshake->buffer;
        /* array to pointer decay for handshake->cb_data */
        handle->args.data        = handshake->cb_data;
        handle->args.size        = handshake->size;
        handle->args.remote      = req->rank;
        handle->type             = LCI_PUT_TARGET;

        lci_ce_debug_verbose("Put Req recv:\t%d -> %d(%p) size %zu with tag %d, cb data %p",
                             handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag, (void *)handle->args.data);

        /* get request from pool and set context to callback handle */
        lci_req_handle_t *req_handle = parsec_thread_mempool_allocate(
                                              lci_req_mempool.thread_mempools);
        lc_req *recv_req = &req_handle->req;
        recv_req->ctx = handle;

        /* start receive for the put */
        RETRY(lc_recv(handshake->buffer, handshake->size, req->rank, req->meta,
                      put_ep, recv_req));
        lci_ce_debug_verbose("Put Recv start:\t%d -> %d(%p) size %zu with tag %d",
                             handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);

        lc_cq_reqfree(put_am_ep, req);
        ret++;
    }

    /* put receive - at target */
    while (LC_OK == lc_cq_pop(put_ep, &req)) {
        /* get callback handle from request context */
        lci_cb_handle_t *handle = req->ctx;
        lci_ce_debug_verbose("Put Recv end:\t%d -> %d(%p) size %zu with tag %d",
                             handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);
#if 0
        handle->cb.put_target(handle->args.comm_engine,
                              handle->args.tag,  handle->args.msg,
                              handle->args.size, handle->args.remote,
                              handle->args.data);
#endif
        /* API seems to be bugged, pass callback data as message */
        handle->cb.put_target(handle->args.comm_engine,
                              handle->args.tag,  handle->args.data,
                              handle->args.size, handle->args.remote,
                              NULL);
        lci_ce_debug_verbose("called %p", (void *)handle->cb.put_target);

        /* return memory from AM */
        /* handle->args.data points to the cb_data field of the handshake info
         * which is the data field of the dynmsg object */
        lci_handshake_t *handshake = container_of(handle->args.data, lci_handshake_t, cb_data);
        lci_dynmsg_t *dynmsg = container_of(handshake, lci_dynmsg_t, data);
        parsec_thread_mempool_free(dynmsg->mempool_owner, dynmsg);

        /* return handle and request to pools */
        parsec_thread_mempool_free(handle->mempool_owner, handle);
        lc_cq_reqfree(put_ep, req); // returns packet to pool, not req
        lci_req_handle_t *req_handle = container_of(req, lci_req_handle_t, req);
        parsec_thread_mempool_free(req_handle->mempool_owner, req_handle);
        ret++;
    }

    /* put send - at origin */
    for (parsec_list_item_t *item = parsec_dequeue_try_pop_front(lci_put_send_queue);
                             item != NULL;
                             item = parsec_dequeue_try_pop_front(lci_put_send_queue)) {
        lci_cb_handle_t *handle = (lci_cb_handle_t *)item;
        lci_ce_debug_verbose("Put Send end:\t%d(%p) -> %d(%p) size %zu",
                             ep_rank,
                             (void *)(((lci_mem_reg_handle_t *)handle->args.lreg)->mem + handle->args.ldispl),
                             handle->args.remote,
                             (void *)(((lci_mem_reg_handle_t *)handle->args.rreg)->mem + handle->args.rdispl),
                             handle->args.size);
        handle->cb.put_origin(handle->args.comm_engine,
                              handle->args.lreg, handle->args.ldispl,
                              handle->args.rreg, handle->args.rdispl,
                              handle->args.size, handle->args.remote,
                              handle->args.data);
        /* return handle to mempool */
        parsec_thread_mempool_free(handle->mempool_owner, handle);
        ret++;
    }

    /* respond to get request */
    while (LC_OK == lc_cq_pop(get_am_ep, &req)) {
        lci_handshake_t *handshake = req->buffer;

        lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                        lci_cb_handle_mempool.thread_mempools);
        handle->cb.get_target    = (parsec_ce_am_callback_t)handshake->cb;
        handle->args.tag         = req->meta;
        handle->args.msg         = handshake->buffer;
        /* array to pointer decay for handshake->cb_data */
        handle->args.data        = handshake->cb_data;
        handle->args.size        = handshake->size;
        handle->args.remote      = req->rank;
        handle->type             = LCI_GET_TARGET;

        lci_ce_debug_verbose("Get Req recv:\t%d <- %d(%p) size %zu with tag %d, cb data %p",
                             handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag, (void *)handle->args.data);

        /* start send for the get */
        RETRY(lc_send(handshake->buffer, handshake->size, req->rank, req->meta,
                      get_ep, lci_get_send_cb, handle));
        lci_ce_debug_verbose("Get Send start:\t%d <- %d(%p) size %zu with tag %d",
                             handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);

        lc_cq_reqfree(get_am_ep, req);
        ret++;
    }

    /* get receive - at origin */
    while (LC_OK == lc_cq_pop(get_ep, &req)) {
        /* get callback handle from request context */
        lci_cb_handle_t *handle = req->ctx;
        lci_ce_debug_verbose("Get Recv end:\t%d(%p) <- %d(%p) size %zu with tag %d",
                             ep_rank,
                             (void *)(((lci_mem_reg_handle_t *)handle->args.lreg)->mem + handle->args.ldispl),
                             handle->args.remote,
                             (void *)(((lci_mem_reg_handle_t *)handle->args.rreg)->mem + handle->args.rdispl),
                             handle->args.size, req->meta);
        handle->cb.get_origin(handle->args.comm_engine,
                              handle->args.lreg, handle->args.ldispl,
                              handle->args.rreg, handle->args.rdispl,
                              handle->args.size, handle->args.remote,
                              handle->args.data);

        /* return handle and request to pools */
        parsec_thread_mempool_free(handle->mempool_owner, handle);
        lc_cq_reqfree(get_ep, req); // returns packet to pool, not req
        lci_req_handle_t *req_handle = container_of(req, lci_req_handle_t, req);
        parsec_thread_mempool_free(req_handle->mempool_owner, req_handle);
        ret++;
    }

    /* get send - at target */
    for (parsec_list_item_t *item = parsec_dequeue_try_pop_front(lci_get_send_queue);
                             item != NULL;
                             item = parsec_dequeue_try_pop_front(lci_get_send_queue)) {
        lci_cb_handle_t *handle = (lci_cb_handle_t *)item;
        lci_ce_debug_verbose("Get Send end:\t%d <- %d(%p) size %zu with tag %d",
                             handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);
#if 0
        handle->cb.get_target(handle->args.comm_engine,
                              handle->args.tag,  handle->args.msg,
                              handle->args.size, handle->args.remote,
                              handle->args.data);
#endif
        /* API seems to be bugged, pass callback data as message */
        handle->cb.get_target(handle->args.comm_engine,
                              handle->args.tag,  handle->args.data,
                              handle->args.size, handle->args.remote,
                              NULL);

        /* return memory from AM */
        /* handle->args.data points to the cb_data field of the handshake info
         * which is the data field of the dynmsg object */
        lci_handshake_t *handshake = container_of(handle->args.data, lci_handshake_t, cb_data);
        lci_dynmsg_t *dynmsg = container_of(handshake, lci_dynmsg_t, data);
        parsec_thread_mempool_free(dynmsg->mempool_owner, dynmsg);

        /* return handle to mempool */
        parsec_thread_mempool_free(handle->mempool_owner, handle);
        ret++;
    }

    return ret;
}
#endif

/* restarts progress thread, if paused */
int
lci_enable(parsec_comm_engine_t *comm_engine)
{
#if 0
    int ret;
    pthread_mutex_lock(&progress_mutex);
    if (ret = (progress_status == PAUSE)) {
        progress_status = RUN;
        pthread_cond_signal(&progress_condition);
    }
    pthread_mutex_unlock(&progress_mutex);
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
    pthread_mutex_lock(&progress_mutex);
    if (ret = (progress_status == RUN)) {
        progress_status = PAUSE;
    }
    pthread_mutex_unlock(&progress_mutex);
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
    lci_ce_debug_verbose("pack %p(%d) into %p(%d) + %d",
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
    lci_ce_debug_verbose("unpack %p(%d) + %d into %p(%d)",
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
    if (rc != PARSEC_SUCCESS);
        return 0;
    size_t bytes = size * count;
    uint8_t *dst_buf = (uint8_t *)PARSEC_DATA_COPY_GET_PTR(dst) + displ_dst;
    uint8_t *src_buf = (uint8_t *)PARSEC_DATA_COPY_GET_PTR(src) + displ_src;
    PARSEC_DEBUG_VERBOSE(20, parsec_debug_output,
                         "LCI[%d]:\treshape %p into %p: %"PRIu64" x datatype(%p)",
                         ep_rank, src_buf, dst_buf, count, (void *)layout);
    memcpy(dst_buf, src_buf, bytes);
    return 1;
}

int
lci_sync(parsec_comm_engine_t *comm_engine)
{
    lci_ce_debug_verbose("sync");
    lc_barrier(collective_ep);
    return 1;
}

int
lci_can_push_more(parsec_comm_engine_t *comm_engine)
{
    return 1;
}
