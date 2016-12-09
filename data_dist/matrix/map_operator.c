/*
 * Copyright (c) 2011-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#include "parsec.h"
#include "parsec/debug.h"
#include "parsec/remote_dep.h"
#include "matrix.h"
#include "parsec/parsec_prof_grapher.h"
#include "parsec/scheduling.h"
#include "parsec/datarepo.h"
#include "parsec/devices/device.h"
#include "parsec/vpmap.h"
#include "parsec/data_internal.h"
#include "parsec/interfaces/interface.h"

#if defined(PARSEC_PROF_TRACE)
int parsec_map_operator_profiling_array[2] = {-1};
#define TAKE_TIME(context, key, eid, refdesc, refid) do {   \
   parsec_profile_ddesc_info_t info;                         \
   info.desc = (parsec_ddesc_t*)refdesc;                     \
   info.id = refid;                                         \
   PARSEC_PROFILING_TRACE(context->eu_profile,               \
                         __parsec_handle->super.super.profiling_array[(key)],\
                         eid, __parsec_handle->super.super.handle_id, (void*)&info);  \
  } while(0);
#else
#define TAKE_TIME(context, key, id, refdesc, refid)
#endif

typedef struct parsec_map_operator_handle {
    parsec_handle_t             super;
    const tiled_matrix_desc_t* src;
          tiled_matrix_desc_t* dest;
    volatile uint32_t          next_k;
    parsec_operator_t           op;
    void*                      op_data;
} parsec_map_operator_handle_t;

typedef struct __parsec_map_operator_handle {
    parsec_map_operator_handle_t super;
} __parsec_map_operator_handle_t;

static const parsec_flow_t flow_of_map_operator;
static const parsec_function_t parsec_map_operator;

#define src(k,n)  (((parsec_ddesc_t*)__parsec_handle->super.src)->data_of((parsec_ddesc_t*)__parsec_handle->super.src, (k), (n)))
#define dest(k,n)  (((parsec_ddesc_t*)__parsec_handle->super.dest)->data_of((parsec_ddesc_t*)__parsec_handle->super.dest, (k), (n)))

#if defined(PARSEC_PROF_TRACE)
static inline uint32_t map_operator_op_hash(const __parsec_map_operator_handle_t *o, int k, int n )
{
    return o->super.src->mt * k + n;
}
#endif  /* defined(PARSEC_PROF_TRACE) */

static inline int minexpr_of_row_fct(const parsec_handle_t *__parsec_handle_parent, const assignment_t *assignments)
{
    const __parsec_map_operator_handle_t *__parsec_handle = (const __parsec_map_operator_handle_t*)__parsec_handle_parent;
    (void)assignments;
    return __parsec_handle->super.src->i;
}
static const expr_t minexpr_of_row = {
    .op = EXPR_OP_INLINE,
    .u_expr = { .inline_func_int32 = minexpr_of_row_fct }
};
static inline int maxexpr_of_row_fct(const parsec_handle_t *__parsec_handle_parent, const assignment_t *assignments)
{
    const __parsec_map_operator_handle_t *__parsec_handle = (const __parsec_map_operator_handle_t*)__parsec_handle_parent;

    (void)__parsec_handle;
    (void)assignments;
    return __parsec_handle->super.src->mt;
}
static const expr_t maxexpr_of_row = {
    .op = EXPR_OP_INLINE,
    .u_expr = { .inline_func_int32 = maxexpr_of_row_fct }
};
static const symbol_t symb_row = {
    .min = &minexpr_of_row,
    .max = &maxexpr_of_row,
    .flags = PARSEC_SYMBOL_IS_STANDALONE
};

static inline int minexpr_of_column_fct(const parsec_handle_t *__parsec_handle_parent, const assignment_t *assignments)
{
    const __parsec_map_operator_handle_t *__parsec_handle = (const __parsec_map_operator_handle_t*)__parsec_handle_parent;
    (void)assignments;
    return __parsec_handle->super.src->j;
}

static const expr_t minexpr_of_column = {
    .op = EXPR_OP_INLINE,
    .u_expr = { .inline_func_int32 = minexpr_of_column_fct }
};

static inline int maxexpr_of_column_fct(const parsec_handle_t *__parsec_handle_parent, const assignment_t *assignments)
{
    const __parsec_map_operator_handle_t *__parsec_handle = (const __parsec_map_operator_handle_t*)__parsec_handle_parent;

    (void)__parsec_handle;
    (void)assignments;
    return __parsec_handle->super.src->nt;
}
static const expr_t maxexpr_of_column = {
    .op = EXPR_OP_INLINE,
    .u_expr = { .inline_func_int32 = maxexpr_of_column_fct }
};
static const symbol_t symb_column = {
    .min = &minexpr_of_column,
    .max = &maxexpr_of_column,
    .flags = PARSEC_SYMBOL_IS_STANDALONE
};

static inline int affinity_of_map_operator(parsec_execution_context_t *this_task,
                                           parsec_data_ref_t *ref)
{
    const __parsec_map_operator_handle_t *__parsec_handle = (const __parsec_map_operator_handle_t*)this_task->parsec_handle;
    int k = this_task->locals[0].value;
    int n = this_task->locals[1].value;
    ref->ddesc = (parsec_ddesc_t*)__parsec_handle->super.src;
    ref->key = ref->ddesc->data_key(ref->ddesc, k, n);
    return 1;
}

static inline int initial_data_of_map_operator(parsec_execution_context_t *this_task,
                                               parsec_data_ref_t *refs)
{
    int __flow_nb = 0;
    parsec_ddesc_t *__d;
    const __parsec_map_operator_handle_t *__parsec_handle = (const __parsec_map_operator_handle_t*)this_task->parsec_handle;
    int k = this_task->locals[0].value;
    int n = this_task->locals[1].value;

    __d = (parsec_ddesc_t*)__parsec_handle->super.src;
    refs[__flow_nb].ddesc = __d;
    refs[__flow_nb].key = __d->data_key(__d, k, n);
    __flow_nb++;

    return __flow_nb;
}

static inline int final_data_of_map_operator(parsec_execution_context_t *this_task,
                                             parsec_data_ref_t *data_refs)
{
    int __flow_nb = 0;
    parsec_ddesc_t *__d;
    const __parsec_map_operator_handle_t *__parsec_handle = (const __parsec_map_operator_handle_t*)this_task->parsec_handle;
    int k = this_task->locals[0].value;
    int n = this_task->locals[1].value;

    __d = (parsec_ddesc_t*)__parsec_handle->super.dest;
    data_refs[__flow_nb].ddesc = __d;
    data_refs[__flow_nb].key = __d->data_key(__d, k, n);
    __flow_nb++;

    return __flow_nb;
}

static const dep_t flow_of_map_operator_dep_in = {
    .cond = NULL,
    .function_id = 0,  /* parsec_map_operator.function_id */
    .flow = &flow_of_map_operator,
};

static const dep_t flow_of_map_operator_dep_out = {
    .cond = NULL,
    .function_id = 0,  /* parsec_map_operator.function_id */
    .dep_index = 1,
    .flow = &flow_of_map_operator,
};

static const parsec_flow_t flow_of_map_operator = {
    .name = "I",
    .sym_type = SYM_INOUT,
    .flow_flags = FLOW_ACCESS_RW,
    .flow_index = 0,
    .dep_in  = { &flow_of_map_operator_dep_in },
    .dep_out = { &flow_of_map_operator_dep_out }
};

static parsec_ontask_iterate_t
add_task_to_list(parsec_execution_unit_t *eu_context,
                 const parsec_execution_context_t *newcontext,
                 const parsec_execution_context_t *oldcontext,
                 const dep_t* dep,
                 parsec_dep_data_description_t* data,
                 int rank_src, int rank_dst,
                 int vpid_dst,
                 void *_ready_lists)
{
    parsec_execution_context_t** pready_list = (parsec_execution_context_t**)_ready_lists;
    parsec_execution_context_t* new_context = (parsec_execution_context_t*)parsec_thread_mempool_allocate( eu_context->context_mempool );

    new_context->status = PARSEC_TASK_STATUS_NONE;
    PARSEC_COPY_EXECUTION_CONTEXT(new_context, newcontext);
    pready_list[vpid_dst] = (parsec_execution_context_t*)parsec_list_item_ring_push_sorted( (parsec_list_item_t*)(pready_list[vpid_dst]),
                                                                                          (parsec_list_item_t*)new_context,
                                                                                          parsec_execution_context_priority_comparator );

    (void)oldcontext; (void)dep; (void)rank_src; (void)rank_dst; (void)vpid_dst; (void)data;
    return PARSEC_ITERATE_STOP;
}

static void iterate_successors(parsec_execution_unit_t *eu,
                               const parsec_execution_context_t *this_task,
                               uint32_t action_mask,
                               parsec_ontask_function_t *ontask,
                               void *ontask_arg)
{
    __parsec_map_operator_handle_t *__parsec_handle = (__parsec_map_operator_handle_t*)this_task->parsec_handle;
    int k = this_task->locals[0].value;
    int n = this_task->locals[1].value+1;
    parsec_execution_context_t nc;

    nc.priority = 0;
    nc.chore_id = 0;
    nc.data[0].data_repo = NULL;
    nc.data[1].data_repo = NULL;
    /* If this is the last n, try to move to the next k */
    for( ; k < (int)__parsec_handle->super.src->nt; n = 0) {
        for( ; n < (int)__parsec_handle->super.src->mt; n++ ) {
            if( __parsec_handle->super.src->super.myrank !=
                ((parsec_ddesc_t*)__parsec_handle->super.src)->rank_of((parsec_ddesc_t*)__parsec_handle->super.src,
                                                                     k, n) )
                continue;
            int vpid =  ((parsec_ddesc_t*)__parsec_handle->super.src)->vpid_of((parsec_ddesc_t*)__parsec_handle->super.src,
                                                                             k, n);
            /* Here we go, one ready local task */
            nc.locals[0].value = k;
            nc.locals[1].value = n;
            nc.function = &parsec_map_operator /*this*/;
            nc.parsec_handle = this_task->parsec_handle;
            nc.data[0].data_in = this_task->data[0].data_out;
            nc.data[1].data_in = this_task->data[1].data_out;

            ontask(eu, &nc, this_task, &flow_of_map_operator_dep_out, NULL,
                   __parsec_handle->super.src->super.myrank,
                   __parsec_handle->super.src->super.myrank,
                   vpid,
                   ontask_arg);
            return;
        }
        /* Go to the next row ... atomically */
        k = parsec_atomic_inc_32b( &__parsec_handle->super.next_k );
    }
    (void)action_mask;
}

static int release_deps(parsec_execution_unit_t *eu,
                        parsec_execution_context_t *this_task,
                        uint32_t action_mask,
                        parsec_remote_deps_t *deps)
{
    parsec_execution_context_t** ready_list;
    int i;

    ready_list = (parsec_execution_context_t **)calloc(sizeof(parsec_execution_context_t *),
                                                      vpmap_get_nb_vp());

    iterate_successors(eu, this_task, action_mask, add_task_to_list, ready_list);

    if(action_mask & PARSEC_ACTION_RELEASE_LOCAL_DEPS) {
        for(i = 0; i < vpmap_get_nb_vp(); i++) {
            if( NULL != ready_list[i] ) {
                if( i == eu->virtual_process->vp_id )
                    __parsec_schedule(eu, ready_list[i]);
                else
                    __parsec_schedule(eu->virtual_process->parsec_context->virtual_processes[i]->execution_units[0],
                                     ready_list[i]);
            }
        }
    }

    if(action_mask & PARSEC_ACTION_RELEASE_LOCAL_REFS) {
        /**
         * There is no repo to be release in this instance, so instead just release the
         * reference of the data copy.
         *
         * data_repo_entry_used_once( eu, this_task->data[0].data_repo, this_task->data[0].data_repo->key );
         */
        PARSEC_DATA_COPY_RELEASE(this_task->data[0].data_in);
    }

    free(ready_list);

    (void)deps;
    return 1;
}

static int data_lookup(parsec_execution_unit_t *context,
                       parsec_execution_context_t *this_task)
{
    const __parsec_map_operator_handle_t *__parsec_handle = (__parsec_map_operator_handle_t*)this_task->parsec_handle;
    int k = this_task->locals[0].value;
    int n = this_task->locals[1].value;

    (void)context;

    if( NULL != __parsec_handle->super.src ) {
        this_task->data[0].data_in   = parsec_data_get_copy(src(k,n), 0);
        this_task->data[0].data_repo = NULL;
        this_task->data[0].data_out  = NULL;
    }
    if( NULL != __parsec_handle->super.dest ) {
        this_task->data[1].data_in   = parsec_data_get_copy(dest(k,n), 0);
        this_task->data[1].data_repo = NULL;
        this_task->data[1].data_out  = this_task->data[1].data_in;
    }
    return 0;
}

static int hook_of(parsec_execution_unit_t *context,
                   parsec_execution_context_t *this_task)
{
    const __parsec_map_operator_handle_t *__parsec_handle = (const __parsec_map_operator_handle_t*)this_task->parsec_handle;
    int k = this_task->locals[0].value;
    int n = this_task->locals[1].value;
    const void* src_data = NULL;
    void* dest_data = NULL;

    if( NULL != __parsec_handle->super.src ) {
        src_data = PARSEC_DATA_COPY_GET_PTR(this_task->data[0].data_in);
    }
    if( NULL != __parsec_handle->super.dest ) {
        dest_data = PARSEC_DATA_COPY_GET_PTR(this_task->data[1].data_in);
    }

#if !defined(PARSEC_PROF_DRY_BODY)
    TAKE_TIME(context, 2*this_task->function->function_id,
              map_operator_op_hash( __parsec_handle, k, n ), __parsec_handle->super.src,
              ((parsec_ddesc_t*)(__parsec_handle->super.src))->data_key((parsec_ddesc_t*)__parsec_handle->super.src, k, n) );
    __parsec_handle->super.op( context, src_data, dest_data, __parsec_handle->super.op_data, k, n );
#endif
    (void)context;
    return 0;
}

static int complete_hook(parsec_execution_unit_t *context,
                         parsec_execution_context_t *this_task)
{
    const __parsec_map_operator_handle_t *__parsec_handle = (const __parsec_map_operator_handle_t *)this_task->parsec_handle;
    int k = this_task->locals[0].value;
    int n = this_task->locals[1].value;
    (void)k; (void)n; (void)__parsec_handle;

    TAKE_TIME(context, 2*this_task->function->function_id+1, map_operator_op_hash( __parsec_handle, k, n ), NULL, 0);

#if defined(PARSEC_PROF_GRAPHER)
    parsec_prof_grapher_task(this_task, context->th_id, context->virtual_process->vp_id, k+n);
#endif  /* defined(PARSEC_PROF_GRAPHER) */

    release_deps(context, this_task,
                 (PARSEC_ACTION_RELEASE_REMOTE_DEPS |
                  PARSEC_ACTION_RELEASE_LOCAL_DEPS |
                  PARSEC_ACTION_RELEASE_LOCAL_REFS |
                  PARSEC_ACTION_DEPS_MASK),
                 NULL);

    return 0;
}

static __parsec_chore_t __parsec_map_chores[] = {
    { .type     = PARSEC_DEV_CPU,
      .evaluate = NULL,
      .hook     = hook_of },
    { .type     = PARSEC_DEV_NONE,
      .evaluate = NULL,
      .hook     = NULL },
};

static const parsec_function_t parsec_map_operator = {
    .name = "map_operator",
    .flags = 0x0,
    .function_id = 0,
    .nb_parameters = 2,
    .nb_locals = 2,
    .dependencies_goal = 0x1,
    .params = { &symb_row, &symb_column },
    .locals = { &symb_row, &symb_column },
    .data_affinity = affinity_of_map_operator,
    .initial_data = initial_data_of_map_operator,
    .final_data = final_data_of_map_operator,
    .priority = NULL,
    .in = { &flow_of_map_operator },
    .out = { &flow_of_map_operator },
    .key = NULL,
    .prepare_input = data_lookup,
    .incarnations = __parsec_map_chores,
    .iterate_successors = iterate_successors,
    .release_deps = release_deps,
    .complete_execution = complete_hook,
    .release_task = parsec_release_task_to_mempool_update_nbtasks,
    .fini = NULL,
};

static void parsec_map_operator_startup_fn(parsec_context_t *context,
                                          parsec_handle_t *parsec_handle,
                                          parsec_execution_context_t** startup_list)
{
    __parsec_map_operator_handle_t *__parsec_handle = (__parsec_map_operator_handle_t*)parsec_handle;
    parsec_execution_context_t fake_context;
    parsec_execution_context_t *ready_list;
    int k = 0, n = 0, count = 0, vpid = 0;
    parsec_execution_unit_t* eu;

    *startup_list = NULL;
    fake_context.function = &parsec_map_operator;
    fake_context.parsec_handle = parsec_handle;
    fake_context.priority = 0;
    fake_context.data[0].data_repo = NULL;
    fake_context.data[0].data_in   = NULL;
    fake_context.data[1].data_repo = NULL;
    fake_context.data[1].data_in   = NULL;
    for( vpid = 0; vpid < vpmap_get_nb_vp(); vpid++ ) {
        /* If this is the last n, try to move to the next k */
        count = 0;
        for( ; k < (int)__parsec_handle->super.src->nt; n = 0) {
            for( ; n < (int)__parsec_handle->super.src->mt; n++ ) {
                if (__parsec_handle->super.src->super.myrank !=
                    ((parsec_ddesc_t*)__parsec_handle->super.src)->rank_of((parsec_ddesc_t*)__parsec_handle->super.src,
                                                                         k, n) )
                    continue;

                if( vpid != ((parsec_ddesc_t*)__parsec_handle->super.src)->vpid_of((parsec_ddesc_t*)__parsec_handle->super.src,
                                                                                 k, n) )
                    continue;
                /* Here we go, one ready local task */
                ready_list = NULL;
                eu = context->virtual_processes[vpid]->execution_units[count];
                fake_context.locals[0].value = k;
                fake_context.locals[1].value = n;
                add_task_to_list(eu, &fake_context, NULL, &flow_of_map_operator_dep_out, NULL,
                                 __parsec_handle->super.src->super.myrank, -1,
                                 0, (void*)&ready_list);
                __parsec_schedule( eu, ready_list );
                count++;
                if( count == context->virtual_processes[vpid]->nb_cores )
                    goto done;
                break;
            }
            /* Go to the next row ... atomically */
            k = parsec_atomic_inc_32b( &__parsec_handle->super.next_k );
        }
    done:  continue;
    }
    return;
}

/**
 * Apply the operator op on all tiles of the src matrix. The src matrix is const, the
 * result is supposed to be pushed on the dest matrix. However, any of the two matrices
 * can be NULL, and then the data is reported as NULL in the corresponding op
 * floweter.
 */
parsec_handle_t*
parsec_map_operator_New(const tiled_matrix_desc_t* src,
                       tiled_matrix_desc_t* dest,
                       parsec_operator_t op,
                       void* op_data)
{
    __parsec_map_operator_handle_t *res = (__parsec_map_operator_handle_t*)calloc(1, sizeof(__parsec_map_operator_handle_t));

    if( (NULL == src) && (NULL == dest) )
        return NULL;
    /* src and dest should have similar distributions */
    /* TODO */

    res->super.src     = src;
    res->super.dest    = dest;
    res->super.op      = op;
    res->super.op_data = op_data;

#  if defined(PARSEC_PROF_TRACE)
    res->super.super.profiling_array = parsec_map_operator_profiling_array;
    if( -1 == parsec_map_operator_profiling_array[0] ) {
        parsec_profiling_add_dictionary_keyword("operator", "fill:CC2828",
                                               sizeof(parsec_profile_ddesc_info_t), PARSEC_PROFILE_DDESC_INFO_CONVERTOR,
                                               (int*)&res->super.super.profiling_array[0 + 2 * parsec_map_operator.function_id],
                                               (int*)&res->super.super.profiling_array[1 + 2 * parsec_map_operator.function_id]);
    }
#  endif /* defined(PARSEC_PROF_TRACE) */

    res->super.super.handle_id = 1111;
    res->super.super.nb_tasks = src->nb_local_tiles;
    res->super.super.nb_pending_actions = 1;  /* for all local tasks */
    res->super.super.startup_hook = parsec_map_operator_startup_fn;
    (void)parsec_handle_reserve_id((parsec_handle_t *)res);
    return (parsec_handle_t*)res;
}

void parsec_map_operator_Destruct( parsec_handle_t* o )
{
    free(o);
}
