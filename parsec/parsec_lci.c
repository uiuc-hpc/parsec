/*
 * Copyright (c) 2009-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

/* C standard library */
#include <assert.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>

/* External libraries */
#include <lci.h>

/* PaRSEC */
#include "parsec/class/list_item.h"
#include "parsec/class/parsec_hash_table.h"
#include "parsec/class/parsec_object.h"
#include "parsec/mempool.h"
#include "parsec/parsec_comm_engine.h"
#include "parsec/parsec_lci.h"
#include "parsec/runtime.h"

/* ------- LCI implementation below ------- */

/* LCI memory handle type
 * PaRSEC object, inherits from parsec_list_item_t
 */
typedef struct lci_mem_reg_handle_s {
    parsec_list_item_t      super;
    parsec_thread_mempool_t *mempool_owner;
    unsigned char           *mem;
    size_t                  size;
    size_t                  count;
    parsec_datatype_t       datatype;
} lci_mem_reg_handle_t;
PARSEC_DECLSPEC OBJ_CLASS_DECLARATION(lci_mem_reg_handle_t);
OBJ_CLASS_INSTANCE(lci_mem_reg_handle_t, parsec_list_item_t, NULL, NULL);

/* memory pool for memory handles */
static parsec_mempool_t *lci_mem_reg_handle_mempool = NULL;

/* LCI callback hash table type */
typedef struct lci_cb_handle_s {
    parsec_list_item_t       super;    /* for mempool    */ /* 32 bytes */
    parsec_hash_table_item_t ht_item;  /* for hash table */ /* 24 bytes */
    parsec_thread_mempool_t  *mempool_owner;                /*  8 bytes */
    union {
        parsec_ce_am_callback_t       am;
        parsec_ce_onesided_callback_t onesided;
        parsec_ce_am_callback_t       onesided_am; /* technically not neeeded */
    } cb;       /* callback function */
    struct {
        void                       *data;  /* callback data  */
        parsec_ce_mem_reg_handle_t lreg;   /* local mem handle */
        ptrdiff_t                  ldispl; /* local mem displ */
        parsec_ce_mem_reg_handle_t rreg;   /* remote mem handle */
        ptrdiff_t                  rdispl; /* remote mem displ */
    } args;
} lci_cb_handle_t;
PARSEC_DECLSPEC OBJ_CLASS_DECLARATION(lci_cb_handle_t);
OBJ_CLASS_INSTANCE(lci_cb_handle_t, parsec_list_item_t, NULL, NULL);

/* memory pool for callbacks */
static parsec_mempool_t *lci_cb_handle_mempool = NULL;

/* hash tables for callbacks */
static parsec_hash_table_t *am_cb_hash_table          = NULL;
static parsec_hash_table_t *onesided_cb_hash_table    = NULL;
static parsec_hash_table_t *onesided_am_cb_hash_table = NULL;

static parsec_key_fn_t key_fns = {
    .key_equal = parsec_hash_table_generic_64bits_key_equal,
    .key_print = parsec_hash_table_generic_64bits_key_print,
    .key_hash  = parsec_hash_table_generic_64bits_key_hash
};

/* LCI one-sided activate message handshake type */
typedef struct lci_onesided_handshake_s {
    unsigned char   *buffer;
    size_t          size;
    parsec_ce_tag_t cb;
    bool            put;
} lci_onesided_handshake_t;

/* endpoint and queue for the generic active messages */
LCI_endpoint_t am_ep;
LCI_CQ_t am_cq;
/* endpoint and queue for the one-sided active messages */
LCI_endpoint_t onesided_am_ep;
LCI_CQ_t onesided_am_cq;
/* endpoint and queue for one-sided put/get */
LCI_endpoint_t onesided_ep;
LCI_CQ_t onesided_cq;

_Atomic uint16_t current_tag = 0;

/* Initialize LCI communication engine */
parsec_comm_engine_t *
lci_init(parsec_context_t *parsec_context)
{
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
    parsec_ce.sync                = lci_sync;
    parsec_ce.can_serve           = lci_can_push_more;
    parsec_ce.send_active_message = lci_send_active_message;
    parsec_ce.parsec_context = context;
    parsec_ce.capabilites.sided = 1;
    parsec_ce.capabilites.supports_noncontiguous_datatype = 0;

    /* create a mempool for memory registration */
    lci_mem_reg_handle_mempool = malloc(sizeof(parsec_mempool_t));
    parsec_mempool_construct(lci_mem_reg_handle_mempool,
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

    /* allocated hash tables for callbacks */
    am_cb_hash_table = OBJ_NEW(parsec_hash_table_t);
    onesided_cb_hash_table = OBJ_NEW(parsec_hash_table_t);
    onesided_am_cb_hash_table = OBJ_NEW(parsec_hash_table_t);

    parsec_hash_table_init(am_cb_hash_table,
                           offsetof(lci_cb_handle_t, ht_item),
                           /*nb*/,
                           key_fns,
                           am_cb_hash_table);

    parsec_hash_table_init(onesided_cb_hash_table,
                           offsetof(lci_cb_handle_t, ht_item),
                           /*nb*/,
                           key_fns,
                           onesided_cb_hash_table);

    parsec_hash_table_init(onesided_am_cb_hash_table,
                           offsetof(lci_cb_handle_t, ht_item),
                           /*nb*/,
                           key_fns,
                           onesided_am_cb_hash_table);
#if 0
    int nb;
    tag_hash_table = OBJ_NEW(parsec_hash_table_t);
    for(nb = 1; nb < 16 && (1 << nb) < tag_hash_table_size; nb++) /* nothing */;
    parsec_hash_table_init(tag_hash_table,
                           offsetof(mpi_funnelled_tag_t, ht_item),
                           nb,
                           tag_key_fns,
                           tag_hash_table);
#endif

    /* init LCI */
    LCI_initialize(NULL, NULL);

    /* create completion queues */
    // length argument ignored in LCI implementation??
    LCI_CQ_create(512, &am_cq);
    LCI_CQ_create(512, &onesided_am_cq);
    LCI_CQ_create(512, &onesided_cq);

    /* create properties */
    LCI_PL_t properties;
    LCI_PL_create(&properties);

    /* set properties for active messages */
#if 0
    LCI_PL_set_comm_type(&properties, LCI_COMM_1SIDED);
    LCI_PL_set_msg_type(&properties, LCI_MSG_BUFFERED);
    LCI_PL_set_completion(&properties, LCI_PORT_COMMAND, LCI_COMPLETION_QUEUE);
    LCI_PL_set_completion(&properties, LCI_PORT_MESSAGE, LCI_COMPLETION_QUEUE);
    LCI_PL_set_dynamic(&properties, LCI_STATIC);
#endif
    LCI_PL_set_comm_type(LCI_COMM_1SIDED, &properties);
    LCI_PL_set_msg_type(LCI_MSG_BUFFERED, &properties);
    LCI_PL_set_completion(LCI_PORT_COMMAND, LCI_COMPLETION_QUEUE, &properties);
    LCI_PL_set_completion(LCI_PORT_MESSAGE, LCI_COMPLETION_QUEUE, &properties);
    /* duplicate global endpoint with AM properties */
    //LCI_group_dup(LCI_ENDPOINT_ALL, 0, properties, &am_ep);
    LCI_PL_set_cq(&am_cq, &properties);
    LCI_endpoint_create(0, properties, &am_ep);
    /* duplicate global endpoint with AM properties for one-sided AM */
    //LCI_group_dup(LCI_ENDPOINT_ALL, 0, properties, &onesided_am_ep);
    LCI_PL_set_cq(&onesided_am_cq, &properties);
    LCI_endpoint_create(0, properties, &onesided_am_ep);

    /* set properties for one-sided */
#if 0
    LCI_PL_set_msg_type(&properties, LCI_MSG_DIRECT);
    LCI_PL_set_comm_type(&properties, LCI_COMM_2SIDED);
    LCI_PL_set_match(&properties, LCI_MATCH_RANKTAG);
#endif
    LCI_PL_set_comm_type(LCI_COMM_2SIDED, &properties);
    LCI_PL_set_msg_type(LCI_MSG_DIRECT, &properties);
    LCI_PL_set_match_type(LCI_MATCH_RANKTAG, &properties);
    /* duplicate global endpoint with one-sided properties */
    //LCI_group_dup(LCI_ENDPOINT_ALL, 0, properties, &onesided_ep); // LCI_ENDPOINT_INIT? ensure correct device
    LCI_PL_set_cq(&onesided_cq, &properties);
    LCI_endpoint_create(0, properties, &onesided_ep);

    /* free properties */
    LCI_PL_free(&properties);

    /* set size and rank */
    if (NULL != context) {
        /* size, rank funcs may change */
        context->nb_nodes = LCI_Size();
        context->my_rank = LCI_Rank();
    }

    return &parsec_ce;
}

/* Finalize LCI communication engine */
int
lci_fini(parsec_comm_engine_t *comm_engine)
{
#if 0
    // LCI_endpoint_free not implemented??? can't free CQ w/o that
    LCI_endpoint_free(am_ep);
    LCI_endpoint_free(onesided_am_ep);
    LCI_endpoint_free(onesided_ep);
    LCI_CQ_free(&am_cq);
    LCI_CQ_free(&onesided_am_cq);
    LCI_CQ_free(&onesided_cq);
#endif
    parsec_hash_table_fini(am_cb_hash_table);
    parsec_hash_table_fini(onesided_cb_hash_table);
    parsec_hash_table_fini(onesided_am_cb_hash_table);
    OBJ_RELEASE(am_cb_hash_table);
    OBJ_RELEASE(onesided_cb_hash_table);
    OBJ_RELEASE(onesided_am_cb_hash_table);

    parsec_mempool_destruct(lci_cb_handle_mempool);
    free(lci_cb_handle_mempool);
    lci_cb_handle_mempool = NULL;

    parsec_mempool_destruct(lci_mem_reg_handle_mempool);
    free(lci_mem_reg_handle_mempool);
    lci_mem_reg_handle_mempool = NULL;

    LCI_finalize();
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
    handle->cb.am = cb;
    handle->args.data = cb_data;
    handle->ht_item.key = key;

    if (NULL != parsec_hash_table_nolock_find(am_cb_hash_table, key)) {
        printf("Tag " PRIu64 " is already registered\n", tag);
        return 0;
    }

    parsec_hash_table_nolock_insert(am_cb_hash_table, &handle->ht_item);
    return 1;
}

int lci_tag_unregister(parsec_ce_tag_t tag)
{
    parsec_key_t key = tag;
    lci_cb_handle_t *handle = parsec_hash_table_remove(am_cb_hash_table, key);
    if (NULL == handle) {
        printf("Tag " PRIu64 " is not registered\n", tag);
        return 0;
    }
    parsec_thread_mempool_free(lci_cb_handle_mempool->thread_mempools, handle);
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
    assert(mem_type == PARSEC_MEM_TYPE_CONTIGUOUS);

    /* allocate from mempool */
    lci_mem_reg_handle_t *handle = parsec_thread_mempool_allocate(
                                 lci_mem_reg_handle_mempool->thread_mempools);
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
    //LCI_unregister(handle->mem);
    parsec_thread_mempool_free(lci_mem_reg_handle_mempool->thread_mempools, handle);
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
    lci_mem_reg_handle_t *handle = (lci_mem_reg_handle_t *) *lreg;
    *mem      = handle->mem;
    *count    = handle->count;
    *datatype = handle->datatype;
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
//    assert(r_cb_data_size + sizeof(lci_onesided_handshake_t) <= LCI_BUFFERED_LENGTH &&
//           "active message data too long);

    /* get next tag */
    uint16_t tag = atomic_fetch_add_explicit(&current_tag, 1,
                                             memory_order_relaxed);

    lci_mem_reg_handle_t *ldata = (lci_mem_reg_handle_t *)lreg;
    lci_mem_reg_handle_t *rdata = (lci_mem_reg_handle_t *)rreg;

    /* set handshake info */
    lci_onesided_handshake_t handshake = { rdata->mem + rdispl, size, r_tag, true };

    /* get LCI bufer */
    //LCI_bdata_t buffer;
    //LCI_buffer_get(onesided_am_ep, &buffer);
    uint8_t buffer[LCI_BUFFERED_LENGTH];
    uint16_t buffer_size = sizeof(lci_onesided_handshake_t) + r_cb_data_size;
    /* pack handshake info and callback data into buffer */
    memcpy(buffer, &handshake, sizeof(lci_onesided_handshake_t));
    memcpy(buffer + sizeof(lci_onesided_handshake_t), r_cb_data, r_cb_data_size);
    /* put handshake to remote, will be retrieved from queue */
    //LCI_putb(onesided_am_ep, buffer, buffer_size, remote, tag, /* r_completion */); // need tag (or add tag to buffer)
    LCI_putmd(buffer, buffer_size, remote, tag, onesided_am_ep);

    /* allocate from mempool */
    lci_cb_handle_t *cb_handle = parsec_thread_mempool_allocate(
                                       lci_cb_handle_mempool->thread_mempools);
    cb_handle->cb.onesided = l_cb;
    cb_handle->args.data   = l_cb_data;
    cb_handle->args.lreg   = lreg;
    cb_handle->args.ldispl = ldispl;
    cb_handle->args.rreg   = rreg;
    cb_handle->args.rdipsl = rdispl;
    cb_handle->ht_item.key = tag;
    /* register local callback and data with tag */
    parsec_hash_table_nolock_insert(onesided_cb_hash_table, &cb_handle->ht_item);

    /* start send to remote with tag */
    //LCI_sendd(onesided_ep, ldata->mem + ldispl, size,
    //          remote, tag, /* completion = NULL */);
    LCI_sendd(ldata->mem + ldispl, size, remote, tag, onesided_ep, NULL);
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
//    assert(r_cb_data_size + sizeof(parsec_ce_tag_t) <= LCI_BUFFERED_LENGTH &&
//           "active message data too long);

    /* get next tag */
    uint16_t tag = atomic_fetch_add_explicit(&current_tag, 1,
                                             memory_order_relaxed);

    lci_mem_reg_handle_t *ldata = (lci_mem_reg_handle_t *)lreg;
    lci_mem_reg_handle_t *rdata = (lci_mem_reg_handle_t *)rreg;

    /* set handshake info */
    lci_onesided_handshake_t handshake = { rdata->mem + rdispl, size, r_tag, false };

    /* get LCI bufer */
    //LCI_bdata_t buffer;
    //LCI_buffer_get(onesided_am_ep, &buffer);
    uint8_t buffer[LCI_BUFFERED_LENGTH];
    uint16_t buffer_size = sizeof(lci_onesided_handshake_t) + r_cb_data_size;
    /* pack handshake info and callback data into buffer */
    memcpy(buffer, &handshake, sizeof(lci_onesided_handshake_t));
    memcpy(buffer + sizeof(lci_onesided_handshake_t), r_cb_data, r_cb_data_size);
    /* put handshake to remote, will be retrieved from queue */
    //LCI_putb(ep, buffer, remote, /* r_completion */); // need tag (or add tag to buffer)
    LCI_putmd(buffer, buffer_size, remote, tag, onesided_am_ep);

    /* allocate from mempool */
    lci_cb_handle_t *cb_handle = parsec_thread_mempool_allocate(
                                       lci_cb_handle_mempool->thread_mempools);
    cb_handle->cb.onesided = l_cb;
    cb_handle->args.data   = l_cb_data;
    cb_handle->args.lreg   = lreg;
    cb_handle->args.ldispl = ldispl;
    cb_handle->args.rreg   = rreg;
    cb_handle->args.rdipsl = rdispl;
    cb_handle->ht_item.key = tag;
    /* register local callback and data with tag */
    parsec_hash_table_nolock_insert(onesided_cb_hash_table, &cb_handle->ht_item);

    /* start recieve from remote with tag */
    //LCI_recvd(onesided_ep, ldata->mem + ldispl, size,
    //          remote, tag, /* completion = NULL */);
    LCI_recvd(ldata->mem + ldispl, size, remote, tag, onesided_ep, NULL);
}

int
lci_send_active_message(parsec_comm_engine_t *comm_engine,
                        parsec_ce_tag_t tag,
                        int remote,
                        void *addr, size_t size)
{
//    assert(size <= LCI_BUFFERED_LENGTH && "Active message data too long);
#if 0
    LCI_bdata_t buffer;
    LCI_buffer_get(am_ep, &buffer);
    memcpy(buffer, addr, size);
    LCI_putb(am_ep, buffer, remote, 0); // need tag (or add tag to buffer)
    LCI_buffer_free(buffer);
#endif
    LCI_putmd(addr, size, remote, tag, am_ep);
}

int
lci_progress(parsec_comm_engine_t *comm_engine)
{
    LCI_progress(0, 100);

    LCI_request_t *req = NULL;

    /* deal with active messages */
    while (LCI_OK == LCI_CQ_dequeue(&am_cq, &req)) {
        parsec_key_t key = req->tag;
        lci_cb_handle_t *handle = parsec_hash_table_nolock_find(am_cb_hash_table, key);
        if (NULL != handle) {
            /* should this be done on this thread? */
            handle->cb.am(comm_engine, key, req->data.buffer, req->length,
                          req->rank, handle->args.data);
        } else {
            printf("Tag " PRIu64 " is not registered\n", key);
        }
        LCI_request_free(am_ep, /* wat dis */ 0, &req);
    }

    /* onesided AM */
    while (LCI_OK == LCI_CQ_dequeue(&onesided_am_cq, &req)) {
        lci_onesided_handshake_t *handshake = req->data.buffer;

        /* allocate from mempool */
        lci_cb_handle_t *cb_handle = parsec_thread_mempool_allocate(
                lci_cb_handle_mempool->thread_mempools);
        cb_handle->cb.onesided_am = (parsec_ce_am_callback_t)handshake->cb;
        cb_handle->args.data = (uint8_t *)req->data.buffer + sizeof(lci_onesided_handshake_t);
        cb_handle->ht_item.key = ((parsec_key_t)req->rank << 32) | (parsec_key_t)req->tag;
        /* register local callback and data with tag + rank */
        parsec_hash_table_nolock_insert(onesided_am_cb_hash_table, &cb_handle->ht_item);

        /* need to check return vals */
        if (handshake->put) {
            /* req->rank is doing a put to us, post recv */
            LCI_recvd(handshake->buffer, handshake->size, req->rank, req->tag, onesided_ep, NULL);
        } else {
            /* req->rank is doing a get to us, post send */
            LCI_sendd(handshake->buffer, handshake->size, req->rank, req->tag, onesided_ep, NULL);
        }
        /* does this deallocate req->data.buffer? */
        LCI_request_free(onesided_am_ep, /* wat dis */ 0, &req);
    }

    /* onesided */
    while (LCI_OK == LCI_CQ_dequeue(&onesided_cq, &req)) {
        parsec_key_t key = 0;
        lci_cb_handle_t *handle = NULL;
        /* THIS ONLY HANDLES PUT
         * need to figure out how to handle both
         * how to differentiate between send because of local put
         * and send because of remote get (and recv/remote put vs local get)
         */
        if (INVALID == req->type) {
            /* command port */
            /* find callback handle with tag, remove it from table */
            key = req->tag;
            handle = parsec_hash_table_remove(onesided_cb_hash_table, key);
            assert(NULL != handle);

            /* should this be done on this thread? */
            handle->cb.onesided(comm_engine,
                                handle->args.lreg, handle->args.ldipsl,
                                handle->args.rreg, handle->args.rdispl,
                                req->length, req->rank,
                                handle->args.data);
        } else {
            /* message port */
            /* find callback handle with tag + rank, remove it from table */
            key = ((parsec_key_t)req->rank << 32) | (parsec_key_t)req->tag;
            handle = parsec_hash_table_remove(onesided_am_cb_hash_table, key);
            assert(NULL != handle);

            /* should this be done on this thread? */
            handle->cb.onesided_am(comm_engine, req->tag, req->data.buffer,
                                   req->length, req->rank, handle->args.data);
        }
        /* return handle to mempool */
        parsec_thread_mempool_free(lci_cb_handle_mempool->thread_mempools, handle);
        LCI_request_free(am_ep, /* wat dis */ 0, &req);
    }
#if 0
    // multiple dequeue not implemented
    LCI_request_t req[10];
    size_t count = LCI_CQ_mul_dequeue(am_cq, req, 10);
    for (size_t i = 0; i < count; i++) {
        if (req[i].type == invalid) {
            /* from command port, i.e. local completion of send or recieve */
        } else {
            /* from message port, i.e. remote completion of active message */
        }
    }
#endif
}

int
lci_enable(parsec_comm_engine_t *comm_engine);

int
lci_disable(parsec_comm_engine_t *comm_engine);

int
lci_pack(parsec_comm_engine_t *comm_engine,
         void *inbuf, int incount,
         void *outbuf, int outsize,
         int *positionA);

int
lci_unpack(parsec_comm_engine_t *comm_engine,
           void *inbuf, int insize, int *position,
           void *outbuf, int outcount);

int
lci_sync(parsec_comm_engine_t *comm_engine);

int
lci_can_push_more(parsec_comm_engine_t *comm_engine);

#endif /* __USE_PARSEC_LCI_H__ */
