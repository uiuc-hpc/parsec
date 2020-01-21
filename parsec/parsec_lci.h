/*
 * Copyright (c) 2009-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */
#ifndef __USE_PARSEC_LCI_H__
#define __USE_PARSEC_LCI_H__

#include "parsec/parsec_comm_engine.h"

/* ------- LCI implementation below ------- */
parsec_comm_engine_t * lci_init(parsec_context_t *parsec_context);
int lci_fini(parsec_comm_engine_t *comm_engine);

int lci_tag_register(parsec_ce_tag_t tag,
                     parsec_ce_am_callback_t cb,
                     void *cb_data,
                     size_t msg_length);

int lci_tag_unregister(parsec_ce_tag_t tag);

int lci_mem_register(void *mem, parsec_mem_type_t mem_type,
                     size_t count, parsec_datatype_t datatype,
                     size_t mem_size,
                     parsec_ce_mem_reg_handle_t *lreg,
                     size_t *lreg_size);

int lci_mem_unregister(parsec_ce_mem_reg_handle_t *lreg);

int lci_get_mem_reg_handle_size(void);

int lci_mem_retrieve(parsec_ce_mem_reg_handle_t lreg, void **mem, parsec_datatype_t *datatype, int *count);

int lci_put(parsec_comm_engine_t *comm_engine,
            parsec_ce_mem_reg_handle_t lreg,
            ptrdiff_t ldispl,
            parsec_ce_mem_reg_handle_t rreg,
            ptrdiff_t rdispl,
            size_t size,
            int remote,
            parsec_ce_onesided_callback_t l_cb, void *l_cb_data,
            parsec_ce_tag_t r_tag, void *r_cb_data, size_t r_cb_data_size);

int lci_get(parsec_comm_engine_t *comm_engine,
            parsec_ce_mem_reg_handle_t lreg,
            ptrdiff_t ldispl,
            parsec_ce_mem_reg_handle_t rreg,
            ptrdiff_t rdispl,
            size_t size,
            int remote,
            parsec_ce_onesided_callback_t l_cb, void *l_cb_data,
            parsec_ce_tag_t r_tag, void *r_cb_data, size_t r_cb_data_size);

int lci_send_active_message(parsec_comm_engine_t *comm_engine,
                            parsec_ce_tag_t tag,
                            int remote,
                            void *addr, size_t size);

_Noreturn void lci_abort(int exit_code);

int lci_progress(parsec_comm_engine_t *comm_engine);

int lci_enable(parsec_comm_engine_t *comm_engine);
int lci_disable(parsec_comm_engine_t *comm_engine);

int lci_pack(parsec_comm_engine_t *comm_engine,
             void *inbuf, int incount,
             void *outbuf, int outsize,
             int *positionA);

int lci_unpack(parsec_comm_engine_t *comm_engine,
               void *inbuf, int insize, int *position,
               void *outbuf, int outcount);

int lci_sync(parsec_comm_engine_t *comm_engine);

int
lci_can_push_more(parsec_comm_engine_t *comm_engine);

#endif /* __USE_PARSEC_LCI_H__ */
