/*
 * Copyright (c) 2013-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#ifndef DEBUG_MARKS_H_HAS_BEEN_INCLUDED
#define DEBUG_MARKS_H_HAS_BEEN_INCLUDED

/**  @addtogroup parsec_internal_debug
 *   @{
 */

#include "parsec/parsec_config.h"

#ifdef PARSEC_DEBUG_HISTORY

struct parsec_task_s;
void debug_mark_exe(int th, int vp, const struct parsec_task_s* ctx);
#define DEBUG_MARK_EXE(th, vp, ctx) debug_mark_exe(th, vp, ctx)

struct remote_dep_wire_activate_s;
void debug_mark_ctl_msg_activate_sent(int to, const void *b, const struct remote_dep_wire_activate_s *m);
#define DEBUG_MARK_CTL_MSG_ACTIVATE_SENT(to, buffer, message) debug_mark_ctl_msg_activate_sent(to, buffer, message)
void debug_mark_ctl_msg_activate_recv(int from, const void *b, const struct remote_dep_wire_activate_s *m);
#define DEBUG_MARK_CTL_MSG_ACTIVATE_RECV(from, buffer, message) debug_mark_ctl_msg_activate_recv(from, buffer, message)

struct remote_dep_wire_get_s;
void debug_mark_ctl_msg_get_sent(int to, const void *b, const struct remote_dep_wire_get_s *m);
#define DEBUG_MARK_CTL_MSG_GET_SENT(to, buffer, message) debug_mark_ctl_msg_get_sent(to, buffer, message)
void debug_mark_ctl_msg_get_recv(int from, const void *b, const struct remote_dep_wire_get_s *m);
#define DEBUG_MARK_CTL_MSG_GET_RECV(from, buffer, message) debug_mark_ctl_msg_get_recv(from, buffer, message)

struct remote_dep_cb_data_s;
void debug_mark_dta_put_start(int to, const struct remote_dep_cb_data_s *cb_data, uintptr_t r_cb_data);
#define DEBUG_MARK_DTA_PUT_START(to, cb_data, r_cb_data) debug_mark_dta_put_start(to, cb_data, r_cb_data)
void debug_mark_dta_put_end(int to, const struct remote_dep_cb_data_s *cb_data);
#define DEBUG_MARK_DTA_PUT_END(to, cb_data) debug_mark_dta_put_end(to, cb_data)
void debug_mark_dta_put_recv(int from, const struct remote_dep_cb_data_s *cb_data);
#define DEBUG_MARK_DTA_PUT_RECV(from, cb_data) debug_mark_dta_put_recv(from, cb_data)


#else /* PARSEC_DEBUG_HISTORY */

#define DEBUG_MARK_EXE(th, vp, ctx)
#define DEBUG_MARK_CTL_MSG_ACTIVATE_SENT(to, buffer, message)
#define DEBUG_MARK_CTL_MSG_ACTIVATE_RECV(from, buffer, message)
#define DEBUG_MARK_CTL_MSG_GET_SENT(to, buffer, message)
#define DEBUG_MARK_CTL_MSG_GET_RECV(from, buffer, message)
#define DEBUG_MARK_DTA_PUT_START(to, cb_data, r_cb_data)
#define DEBUG_MARK_DTA_PUT_END(to, cb_data)
#define DEBUG_MARK_DTA_PUT_RECV(from, cb_data)

#endif /* PARSEC_DEBUG_HISTORY */

/** @} */

#endif /* DEBUG_MARKS_H_HAS_BEEN_INCLUDED */
