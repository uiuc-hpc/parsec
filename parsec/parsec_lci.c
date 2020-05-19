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
#include "parsec/parsec_lci.h"
#include "parsec/parsec_remote_dep.h"
#include "parsec/utils/debug.h"

/* ------- LCI implementation below ------- */

typedef unsigned char byte_t;

//#define RETRY(lci_call) do { } while (LC_OK != (lci_call))
#define RETRY(lci_call)                                                       \
  do {                                                                        \
    while (LC_OK != (lci_call)) {                                             \
      parsec_debug_verbose(20, parsec_debug_output,                           \
                           "LCI[%d]:\tRetrying prior call...", ep_rank);      \
    }                                                                         \
  } while (0)

/* LCI memory handle type
 * PaRSEC object, inherits from parsec_list_item_t
 */
typedef struct lci_mem_reg_handle_s {
    parsec_list_item_t      super;
    parsec_thread_mempool_t *mempool_owner;
    byte_t                  *mem;
    size_t                  size;
    size_t                  count;
    parsec_datatype_t       datatype;
} lci_mem_reg_handle_t;
PARSEC_DECLSPEC OBJ_CLASS_DECLARATION(lci_mem_reg_handle_t);
OBJ_CLASS_INSTANCE(lci_mem_reg_handle_t, parsec_list_item_t, NULL, NULL);

/* memory pool for memory handles */
static parsec_mempool_t lci_mem_reg_handle_mempool;

/* LCI callback hash table type */
typedef struct lci_cb_handle_s {
    parsec_list_item_t       super;    /* for mempool    */ /* 32 bytes */
    parsec_hash_table_item_t ht_item;  /* for hash table */ /* 24 bytes */
    parsec_thread_mempool_t  *mempool_owner;                /*  8 bytes */
    union /* callback function */ {
        parsec_ce_am_callback_t       am;
        parsec_ce_onesided_callback_t onesided;
        parsec_ce_am_callback_t       onesided_am; /* technically not neeeded */
    } cb;
    struct /* callback arguments */ {
        union {
            struct /* onesided callback arguments */ {
                parsec_ce_mem_reg_handle_t lreg;   /* local mem handle  */
                ptrdiff_t                  ldispl; /* local mem displ   */
                parsec_ce_mem_reg_handle_t rreg;   /* remote mem handle */
                ptrdiff_t                  rdispl; /* remote mem displ  */
            };
            struct /* onesided AM callback arguments */ {
                parsec_ce_tag_t      tag;  /* AM tag  */
                void                 *msg; /* message */
            };
        };
        parsec_comm_engine_t *comm_engine; /* comm engine   */
        void                 *data;        /* callback data */
        size_t               size;         /* message size  */
        int                  remote;       /* remote peer   */
    } args;
} lci_cb_handle_t;
PARSEC_DECLSPEC OBJ_CLASS_DECLARATION(lci_cb_handle_t);
OBJ_CLASS_INSTANCE(lci_cb_handle_t, parsec_list_item_t, NULL, NULL);

/* memory pool for callbacks */
static parsec_mempool_t *lci_cb_handle_mempool = NULL;

/* hash table for AM callbacks */
static parsec_hash_table_t *am_cb_hash_table = NULL;

static parsec_key_fn_t key_fns = {
    .key_equal = parsec_hash_table_generic_64bits_key_equal,
    .key_print = parsec_hash_table_generic_64bits_key_print,
    .key_hash  = parsec_hash_table_generic_64bits_key_hash
};

/* LCI request handle (for pool) */
typedef struct lci_req_handle_s {
    parsec_list_item_t      super;
    parsec_thread_mempool_t *mempool_owner;
    lc_req                  req;
} lci_req_handle_t;
OBJ_CLASS_INSTANCE(lci_req_handle_t, parsec_list_item_t, NULL, NULL);

/* memory pool for requests */
static parsec_mempool_t *lci_req_mempool = NULL;

/* LCI dynamic message (for pool) */
typedef struct lci_dynmsg_s {
    parsec_list_item_t      super;
    parsec_thread_mempool_t *mempool_owner;
    alignas(64) byte_t      data[]; /* ensure cache-line alignment */
} lci_dynmsg_t;
OBJ_CLASS_INSTANCE(lci_dynmsg_t, parsec_list_item_t, NULL, NULL);

/* memory pool for dynamic messages */
static parsec_mempool_t *lci_dynmsg_mempool = NULL;

/* queue for completed send callbacks */
static parsec_dequeue_t *lci_put_send_queue = NULL;
static parsec_dequeue_t *lci_get_send_queue = NULL;

/* LCI one-sided activate message handshake type */
typedef struct lci_handshake_s {
    unsigned char   *buffer;
    size_t          size;
    parsec_ce_tag_t cb;
    byte_t          cb_data[];
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

static inline void * dyn_alloc(size_t size, void **ctx)
{
    assert(size <= lc_max_medium(0) && "dynamic message data too long");
    /* if size is 0, return NULL; else allocate from mempool */
    if (size == 0) {
        return NULL;
    }

    lci_dynmsg_t *dynmsg = parsec_thread_mempool_allocate(
                                           lci_dynmsg_mempool->thread_mempools);
    return &dynmsg->data;
}

#if 0
static pthread_cond_t  progress_condition = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t progress_mutex     = PTHREAD_MUTEX_INITIALIZER;
static enum { RUN, PAUSE, STOP } progress_status = RUN;
#endif
static atomic_bool progress_thread_stop = false;
static pthread_t progress_thread_id;

void * lci_progress_thread(void *arg)
{
    parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\tprogress thread start", ep_rank);

    /* bind thread */
    parsec_context_t *context = arg;
    remote_dep_bind_thread(context);

    /* loop until told to stop */
    while (!atomic_load_explicit(&progress_thread_stop, memory_order_acquire)) {
        /* progress until nothing progresses */
        while (lc_progress(0))
            continue;
        /* sleep for comm_yield_ns if set to comm_yield, else yield thread  */
        struct timespec ts = { .tv_sec = 0, .tv_nsec = comm_yield_ns };
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
    parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\tprogress thread stop", ep_rank);
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

    parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\tinit", ep_rank);

    /* Make all the fn pointers point to this component's functions */
    parsec_ce.tag_register        = lci_tag_register;
    parsec_ce.tag_unregister      = lci_tag_unregister;
    parsec_ce.mem_register        = lci_mem_register;
    parsec_ce.mem_unregister      = lci_mem_unregister;
    parsec_ce.get_mem_handle_size = lci_get_mem_reg_handle_size;
    parsec_ce.mem_retrieve        = lci_mem_retrieve;
    parsec_ce.put                 = lci_put;
    parsec_ce.get                 = lci_get;
    parsec_ce.progress            = lci_progress;
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

    /* create a mempool for callbacks */
    lci_cb_handle_mempool = malloc(sizeof(parsec_mempool_t));
    parsec_mempool_construct(lci_cb_handle_mempool,
                             OBJ_CLASS(lci_cb_handle_t),
                             sizeof(lci_cb_handle_t),
                             offsetof(lci_cb_handle_t, mempool_owner),
                             1);

    /* create a mempool for requests */
    lci_req_mempool = malloc(sizeof(parsec_mempool_t));
    parsec_mempool_construct(lci_req_mempool,
                             OBJ_CLASS(lci_req_handle_t),
                             sizeof(lci_req_handle_t),
                             offsetof(lci_req_handle_t, mempool_owner),
                             1);

    /* create a mempool for dynamic messages */
    lci_dynmsg_mempool = malloc(sizeof(parsec_mempool_t));
    parsec_mempool_construct(lci_dynmsg_mempool,
                             OBJ_CLASS(lci_dynmsg_t),
                             /* ensure enough space for any medium message */
                             sizeof(lci_dynmsg_t) + lc_max_medium(0),
                             offsetof(lci_dynmsg_t, mempool_owner),
                             1);

    /* create send callback queues */
    lci_put_send_queue = OBJ_NEW(parsec_dequeue_t);
    lci_get_send_queue = OBJ_NEW(parsec_dequeue_t);

    /* allocated hash tables for callbacks */
    am_cb_hash_table = OBJ_NEW(parsec_hash_table_t);
    parsec_hash_table_init(am_cb_hash_table,
                           offsetof(lci_cb_handle_t, ht_item),
                           4, key_fns, am_cb_hash_table);

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
    parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\tstarting progress thread", ep_rank);
    atomic_store_explicit(&progress_thread_stop, false, memory_order_release);
    pthread_create(&progress_thread_id, NULL, lci_progress_thread, context);

    return &parsec_ce;
}

/* Finalize LCI communication engine */
int
lci_fini(parsec_comm_engine_t *comm_engine)
{
    parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\tfini", ep_rank);
    lci_sync(comm_engine);
    void *progress_retval = NULL;

    /* stop progress thread */
    parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\tstopping progress thread", ep_rank);
    atomic_store_explicit(&progress_thread_stop, true, memory_order_release);
#if 0
    pthread_mutex_lock(&progress_mutex);
    progress_status = STOP;
    pthread_cond_signal(&progress_condition); /* if paused */
    pthread_mutex_unlock(&progress_mutex);
#endif
    pthread_join(progress_thread_id, &progress_retval);

    OBJ_RELEASE(lci_get_send_queue);
    OBJ_RELEASE(lci_put_send_queue);

    parsec_hash_table_fini(am_cb_hash_table);
    OBJ_RELEASE(am_cb_hash_table);

    parsec_mempool_destruct(lci_dynmsg_mempool);
    free(lci_dynmsg_mempool);
    lci_dynmsg_mempool = NULL;

    parsec_mempool_destruct(lci_req_mempool);
    free(lci_req_mempool);
    lci_req_mempool = NULL;

    parsec_mempool_destruct(lci_cb_handle_mempool);
    free(lci_cb_handle_mempool);
    lci_cb_handle_mempool = NULL;

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
                                       lci_cb_handle_mempool->thread_mempools);
    handle->cb.am       = cb;
    handle->args.tag    = tag;
    handle->args.data   = cb_data;
    handle->ht_item.key = key;

    parsec_debug_verbose(20, parsec_debug_output,
                         "LCI[%d]:\tregister Active Message %"PRIu64" data %p size %zu",
                         ep_rank, tag, cb_data, msg_length);
    if (NULL != parsec_hash_table_nolock_find(am_cb_hash_table, key)) {
        parsec_warning("LCI[%d]:\tActive Message %"PRIu64" already registered",
                       ep_rank, tag);
        return 0;
    }

    parsec_hash_table_nolock_insert(am_cb_hash_table, &handle->ht_item);
    return 1;
}

int lci_tag_unregister(parsec_ce_tag_t tag)
{
    parsec_debug_verbose(20, parsec_debug_output,
                         "LCI[%d]:\tunregister Active Message %"PRIu64, ep_rank, tag);
    parsec_key_t key = tag;
    lci_cb_handle_t *handle = parsec_hash_table_remove(am_cb_hash_table, key);
    if (NULL == handle) {
        parsec_warning("LCI[%d]:\tActive Message %"PRIu64" not registered", ep_rank, tag);
        return 0;
    }
    parsec_thread_mempool_free(handle->mempool_owner, handle);
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

    parsec_debug_verbose(20, parsec_debug_output,
                         "LCI[%d]:\tregister memory %p size %zu", ep_rank, mem, mem_size);

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
    parsec_debug_verbose(20, parsec_debug_output,
                         "LCI[%d]:\tunregister memory %p size %zu",
                         ep_rank, (void *)handle->mem, handle->size);
    //LCI_unregister(handle->mem);
    parsec_thread_mempool_free(handle->mempool_owner, handle);
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
    parsec_debug_verbose(20, parsec_debug_output,
                         "LCI[%d]:\tretrieve memory %p size %zu",
                         ep_rank, (void *)handle->mem, handle->size);
    return 1;
}

/* just push onto put queue */
static inline void lci_put_send_cb(void *ctx)
{
    parsec_dequeue_push_back(lci_put_send_queue, ctx);
}

/* just push onto get queue */
static inline void lci_get_send_cb(void *ctx)
{
    parsec_dequeue_push_back(lci_get_send_queue, ctx);
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
    byte_t buffer[buffer_size];
    lci_handshake_t *handshake = (lci_handshake_t *)&buffer;

    /* get next tag */
    int tag = atomic_fetch_add_explicit(&current_tag, 1, memory_order_relaxed);

    lci_mem_reg_handle_t *ldata = (lci_mem_reg_handle_t *)lreg;
    lci_mem_reg_handle_t *rdata = (lci_mem_reg_handle_t *)rreg;

    byte_t *lbuf = ldata->mem + ldispl;
    byte_t *rbuf = rdata->mem + rdispl;

    /* set handshake info */
    handshake->buffer = rbuf;
    handshake->size   = rdata->size;
    handshake->cb     = r_tag;
    memcpy(&handshake->cb_data, r_cb_data, r_cb_data_size);

    /* send handshake to remote, will be retrieved from queue */
    parsec_debug_verbose(20, parsec_debug_output,
                         "LCI[%d]:\tPut Req send:\t%d(%p) -> %d(%p) size %zu with tag %d", ep_rank,
                         ep_rank, lbuf, remote, rbuf, ldata->size, tag);
    RETRY(lc_sendm(handshake, buffer_size, remote, tag, put_am_ep));

    /* allocate from mempool */
    lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                       lci_cb_handle_mempool->thread_mempools);
    handle->cb.onesided      = l_cb;
    handle->args.lreg        = lreg;
    handle->args.ldispl      = ldispl;
    handle->args.rreg        = rreg;
    handle->args.rdispl      = rdispl;
    handle->args.comm_engine = comm_engine;
    handle->args.data        = l_cb_data;
    handle->args.size        = size;
    handle->args.remote      = remote;

    /* start send to remote with tag */
    parsec_debug_verbose(20, parsec_debug_output,
                         "LCI[%d]:\tPut Send start:\t%d(%p) -> %d(%p) size %zu with tag %d", ep_rank,
                         ep_rank, lbuf, remote, rbuf, ldata->size, tag);
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

    byte_t *lbuf = ldata->mem + ldispl;
    byte_t *rbuf = rdata->mem + rdispl;

    /* set handshake info */
    handshake->buffer = rbuf;
    handshake->size   = rdata->size;
    handshake->cb     = r_tag;
    memcpy(&handshake->cb_data, r_cb_data, r_cb_data_size);

    /* send handshake to remote, will be retrieved from queue */
    parsec_debug_verbose(20, parsec_debug_output,
                         "LCI[%d]:\tGet Req send:\t%d(%p) <- %d(%p) size %zu with tag %d", ep_rank,
                         ep_rank, lbuf, remote, rbuf, ldata->size, tag);
    RETRY(lc_sendm(handshake, buffer_size, remote, tag, get_am_ep));

    /* allocate from mempool */
    lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                                       lci_cb_handle_mempool->thread_mempools);
    handle->cb.onesided      = l_cb;
    handle->args.lreg        = lreg;
    handle->args.ldispl      = ldispl;
    handle->args.rreg        = rreg;
    handle->args.rdispl      = rdispl;
    handle->args.comm_engine = comm_engine;
    handle->args.data        = l_cb_data;
    handle->args.size        = size;
    handle->args.remote      = remote;

    /* get request from pool and set context to callback handle */
    lci_req_handle_t *req_handle = parsec_thread_mempool_allocate(
                                             lci_req_mempool->thread_mempools);
    lc_req *req = &req_handle->req;
    req->ctx = handle;

    /* start recieve from remote with tag */
    parsec_debug_verbose(20, parsec_debug_output,
                         "LCI[%d]:\tGet Recv start:\t%d(%p) <- %d(%p) size %zu with tag %d", ep_rank,
                         ep_rank, lbuf, remote, rbuf, ldata->size, tag);
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
    parsec_debug_verbose(20, parsec_debug_output,
                         "LCI[%d]:\tActive Message %"PRIu64" send:\t%d -> %d with message %p size %zu",
                         ep_rank, tag, ep_rank, remote, addr, size);
    RETRY(lc_sendm(addr, size, remote, tag, am_ep));
    return 1;
}

_Noreturn void
lci_abort(int exit_code)
{
    parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\tAbort %d", ep_rank, exit_code);
    for (int i = 0; i < ep_size; i++) {
        if (i != ep_rank) {
            /* send abort message to all other processes */
            parsec_debug_verbose(20, parsec_debug_output,
                                 "LCI[%d]:\tAbort %d send:\t%d -> %d",
                                 ep_rank, exit_code, ep_rank, i);
            RETRY(lc_sends(NULL, 0, i, exit_code, abort_ep));
        }
    }
    /* wait for all processes to ack the abort */
    lc_barrier(collective_ep);
    /* exit without cleaning up */
    _Exit(exit_code);
}

int
lci_progress(parsec_comm_engine_t *comm_engine)
{
    int ret = 0;
    lc_req *req;

    /* handle abort */
    if (LC_OK == lc_cq_pop(abort_ep, &req)) {
        parsec_debug_verbose(20, parsec_debug_output,
                             "LCI[%d]:\tAbort %d recv:\t%d -> %d",
                             ep_rank, req->meta, req->rank, ep_rank);
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
        lci_cb_handle_t *handle = parsec_hash_table_nolock_find(am_cb_hash_table, key);
        parsec_debug_verbose(20, parsec_debug_output,
                             "LCI[%d]:\tActive Message %"PRIu64" recv:\t%d -> %d with message %p size %zu",
                             ep_rank, key, req->rank, ep_rank, req->buffer, req->size);
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
            lci_dynmsg_t *dynmsg = (lci_dynmsg_t *)
                    ((byte_t *)req->buffer - offsetof(lci_dynmsg_t, data));
            parsec_thread_mempool_free(dynmsg->mempool_owner, dynmsg);
        }
        lc_cq_reqfree(am_ep, req);
        ret++;
    }

    /* repond to put request */
    while (LC_OK == lc_cq_pop(put_am_ep, &req)) {
        lci_handshake_t *handshake = req->buffer;

        lci_cb_handle_t *handle = parsec_thread_mempool_allocate(
                lci_cb_handle_mempool->thread_mempools);
        handle->cb.onesided_am   = (parsec_ce_am_callback_t)handshake->cb;
        handle->args.tag         = req->meta;
        handle->args.msg         = handshake->buffer;
        handle->args.comm_engine = comm_engine;
        handle->args.data        = &handshake->cb_data;
        handle->args.size        = handshake->size;
        handle->args.remote      = req->rank;

        parsec_debug_verbose(20, parsec_debug_output,
                             "LCI[%d]:\tPut Req recv:\t%d -> %d(%p) size %zu with tag %d, cb data %p",
                             ep_rank, handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag, (void *)handle->args.data);

        /* get request from pool and set context to callback handle */
        lci_req_handle_t *req_handle = parsec_thread_mempool_allocate(
                                             lci_req_mempool->thread_mempools);
        lc_req *recv_req = &req_handle->req;
        recv_req->ctx = handle;

        /* start receive for the put */
        RETRY(lc_recv(handshake->buffer, handshake->size, req->rank, req->meta,
                      put_ep, recv_req));
        parsec_debug_verbose(20, parsec_debug_output,
                             "LCI[%d]:\tPut Recv start:\t%d -> %d(%p) size %zu with tag %d",
                             ep_rank, handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);

        lc_cq_reqfree(put_am_ep, req);
        ret++;
    }

    /* put receive - at target */
    while (LC_OK == lc_cq_pop(put_ep, &req)) {
        /* get callback handle from request context */
        lci_cb_handle_t *handle = req->ctx;
        parsec_debug_verbose(20, parsec_debug_output,
                             "LCI[%d]:\tPut Recv end:\t%d -> %d(%p) size %zu with tag %d",
                             ep_rank, handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);
        parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\t calling %p", ep_rank, (void *)handle->cb.onesided_am);
#if 0
        handle->cb.onesided_am(handle->args.comm_engine,
                               handle->args.tag,  handle->args.msg,
                               handle->args.size, handle->args.remote,
                               handle->args.data);
#endif
        /* API seems to be bugged, pass callback data as message */
        handle->cb.onesided_am(handle->args.comm_engine,
                               handle->args.tag,  handle->args.data,
                               handle->args.size, handle->args.remote,
                               NULL);
        parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\t called %p", ep_rank, (void *)handle->cb.onesided_am);

        /* return memory from AM */
        /* handle->args.data points to the cb_data field of the handshake info
         * which is the data field of the dynmsg object */
        lci_dynmsg_t *dynmsg = (lci_dynmsg_t *)
                (handle->args.data - offsetof(lci_handshake_t, cb_data)
                                   - offsetof(lci_dynmsg_t, data));
        parsec_thread_mempool_free(dynmsg->mempool_owner, dynmsg);

        /* return handle and request to pools */
        parsec_thread_mempool_free(handle->mempool_owner, handle);
        lc_cq_reqfree(put_ep, req); // returns packet to pool, not req
        lci_req_handle_t *req_handle = (lci_req_handle_t *)
                ((byte_t *)req - offsetof(lci_req_handle_t, req));
        parsec_thread_mempool_free(req_handle->mempool_owner, req_handle);
        ret++;
    }

    /* put send - at origin */
    for (parsec_list_item_t *item = parsec_dequeue_try_pop_front(lci_put_send_queue);
                             item != NULL;
                             item = parsec_dequeue_try_pop_front(lci_put_send_queue)) {
        lci_cb_handle_t *handle = (lci_cb_handle_t *)item;
        parsec_debug_verbose(20, parsec_debug_output,
                             "LCI[%d]:\tPut Send end:\t%d(%p) -> %d(%p) size %zu", ep_rank,
                             ep_rank,
                             (void *)(((lci_mem_reg_handle_t *)handle->args.lreg)->mem + handle->args.ldispl),
                             handle->args.remote,
                             (void *)(((lci_mem_reg_handle_t *)handle->args.rreg)->mem + handle->args.rdispl),
                             handle->args.size);
        handle->cb.onesided(handle->args.comm_engine,
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
                lci_cb_handle_mempool->thread_mempools);
        handle->cb.onesided_am   = (parsec_ce_am_callback_t)handshake->cb;
        handle->args.tag         = req->meta;
        handle->args.msg         = handshake->buffer;
        handle->args.comm_engine = comm_engine;
        handle->args.data        = &handshake->cb_data;
        handle->args.size        = handshake->size;
        handle->args.remote      = req->rank;

        parsec_debug_verbose(20, parsec_debug_output,
                             "LCI[%d]:\tGet Req recv:\t%d <- %d(%p) size %zu with tag %d, cb data %p",
                             ep_rank, handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag, (void *)handle->args.data);

        /* start send for the get */
        RETRY(lc_send(handshake->buffer, handshake->size, req->rank, req->meta,
                      get_ep, lci_get_send_cb, handle));
        parsec_debug_verbose(20, parsec_debug_output,
                             "LCI[%d]:\tGet Send start:\t%d <- %d(%p) size %zu with tag %d",
                             ep_rank, handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);

        lc_cq_reqfree(get_am_ep, req);
        ret++;
    }

    /* get receive - at origin */
    while (LC_OK == lc_cq_pop(get_ep, &req)) {
        /* get callback handle from request context */
        lci_cb_handle_t *handle = req->ctx;
        parsec_debug_verbose(20, parsec_debug_output,
                             "LCI[%d]:\tGet Recv end:\t%d(%p) <- %d(%p) size %zu with tag %d", ep_rank,
                             ep_rank,
                             (void *)(((lci_mem_reg_handle_t *)handle->args.lreg)->mem + handle->args.ldispl),
                             handle->args.remote,
                             (void *)(((lci_mem_reg_handle_t *)handle->args.rreg)->mem + handle->args.rdispl),
                             handle->args.size, req->meta);
        handle->cb.onesided(handle->args.comm_engine,
                            handle->args.lreg, handle->args.ldispl,
                            handle->args.rreg, handle->args.rdispl,
                            handle->args.size, handle->args.remote,
                            handle->args.data);

        /* return handle and request to pools */
        parsec_thread_mempool_free(handle->mempool_owner, handle);
        lc_cq_reqfree(get_ep, req); // returns packet to pool, not req
        lci_req_handle_t *req_handle = (lci_req_handle_t *)
                ((byte_t *)req - offsetof(lci_req_handle_t, req));
        parsec_thread_mempool_free(req_handle->mempool_owner, req_handle);
        ret++;
    }

    /* get send - at target */
    for (parsec_list_item_t *item = parsec_dequeue_try_pop_front(lci_get_send_queue);
                             item != NULL;
                             item = parsec_dequeue_try_pop_front(lci_get_send_queue)) {
        lci_cb_handle_t *handle = (lci_cb_handle_t *)item;
        parsec_debug_verbose(20, parsec_debug_output,
                             "LCI[%d]:\tGet Send end:\t%d <- %d(%p) size %zu with tag %d",
                             ep_rank, handle->args.remote, ep_rank,
                             (void *)handle->args.msg, handle->args.size,
                             handle->args.tag);
#if 0
        handle->cb.onesided_am(handle->args.comm_engine,
                               handle->args.tag,  handle->args.msg,
                               handle->args.size, handle->args.remote,
                               handle->args.data);
#endif
        /* API seems to be bugged, pass callback data as message */
        handle->cb.onesided_am(handle->args.comm_engine,
                               handle->args.tag,  handle->args.data,
                               handle->args.size, handle->args.remote,
                               NULL);

        /* return memory from AM */
        /* handle->args.data points to the cb_data field of the handshake info
         * which is the data field of the dynmsg object */
        lci_dynmsg_t *dynmsg = (lci_dynmsg_t *)
                (handle->args.data - offsetof(lci_handshake_t, cb_data)
                                   - offsetof(lci_dynmsg_t, data));
        parsec_thread_mempool_free(dynmsg->mempool_owner, dynmsg);

        /* return handle to mempool */
        parsec_thread_mempool_free(handle->mempool_owner, handle);
        ret++;
    }

    return ret;
}

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
    parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\tpack %p(%d) into %p(%d) + %d",
                         ep_rank, inbuf, incount, outbuf, outsize, *position);
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
    parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\tunpack %p(%d) + %d into %p(%d)",
                         ep_rank, inbuf, insize, *position, outbuf, outcount);
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
    parsec_debug_verbose(20, parsec_debug_output, "LCI[%d]:\tsync", ep_rank);
    lc_barrier(collective_ep);
    return 1;
}

int
lci_can_push_more(parsec_comm_engine_t *comm_engine)
{
    return 1;
}
