/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      Triad National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "parsec/parsec_config.h"
#include "parsec/datatype/parsec_datatype_config.h"

#include <stddef.h>

#include "parsec/datatype/util/arch.h"
#include "parsec/utils/output.h"
#include "parsec/datatype/parsec_datatype_internal.h"
#include "parsec/datatype/parsec_datatype.h"
#include "parsec/datatype/parsec_convertor_internal.h"
#include "parsec/mca/base/mca_base_var.h"

/* by default the debuging is turned off */
int parsec_datatype_dfd = -1;
bool parsec_ddt_unpack_debug = false;
bool parsec_ddt_pack_debug = false;
bool parsec_ddt_position_debug = false;
bool parsec_ddt_copy_debug = false;
bool parsec_ddt_raw_debug = false;
int parsec_ddt_verbose = -1;  /* Has the datatype verbose it's own output stream */

/* Using this macro implies that at this point _all_ informations needed
 * to fill up the datatype are known.
 * We fill all the static information, the pointer to desc.desc is setup
 * into an array, which is initialized at runtime.
 * Everything is constant.
 */
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_empty =       PARSEC_DATATYPE_INITIALIZER_EMPTY(PARSEC_DATATYPE_FLAG_CONTIGUOUS);

PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_loop =        PARSEC_DATATYPE_INITIALIZER_LOOP(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_end_loop =    PARSEC_DATATYPE_INITIALIZER_END_LOOP(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_lb =          PARSEC_DATATYPE_INITIALIZER_LB(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_ub =          PARSEC_DATATYPE_INITIALIZER_UB(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_int1 =        PARSEC_DATATYPE_INITIALIZER_INT1(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_int2 =        PARSEC_DATATYPE_INITIALIZER_INT2(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_int4 =        PARSEC_DATATYPE_INITIALIZER_INT4(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_int8 =        PARSEC_DATATYPE_INITIALIZER_INT8(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_int16 =       PARSEC_DATATYPE_INITIALIZER_INT16(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_uint1 =       PARSEC_DATATYPE_INITIALIZER_UINT1(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_uint2 =       PARSEC_DATATYPE_INITIALIZER_UINT2(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_uint4 =       PARSEC_DATATYPE_INITIALIZER_UINT4(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_uint8 =       PARSEC_DATATYPE_INITIALIZER_UINT8(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_uint16 =      PARSEC_DATATYPE_INITIALIZER_UINT16(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_float2 =      PARSEC_DATATYPE_INITIALIZER_FLOAT2(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_float4 =      PARSEC_DATATYPE_INITIALIZER_FLOAT4(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_float8 =      PARSEC_DATATYPE_INITIALIZER_FLOAT8(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_float12 =     PARSEC_DATATYPE_INITIALIZER_FLOAT12(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_float16 =     PARSEC_DATATYPE_INITIALIZER_FLOAT16(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_short_float_complex = PARSEC_DATATYPE_INITIALIZER_SHORT_FLOAT_COMPLEX(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_float_complex = PARSEC_DATATYPE_INITIALIZER_FLOAT_COMPLEX(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_double_complex = PARSEC_DATATYPE_INITIALIZER_DOUBLE_COMPLEX(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_long_double_complex = PARSEC_DATATYPE_INITIALIZER_LONG_DOUBLE_COMPLEX(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_bool =        PARSEC_DATATYPE_INITIALIZER_BOOL(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_wchar =       PARSEC_DATATYPE_INITIALIZER_WCHAR(0);
PARSEC_DECLSPEC parsec_datatype_s parsec_datatype_unavailable = PARSEC_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED(UNAVAILABLE, 0);

PARSEC_DECLSPEC dt_elem_desc_t parsec_datatype_predefined_elem_desc[2 * PARSEC_DATATYPE_MAX_PREDEFINED] = {{{{0}}}};

/*
 * NOTE: The order of this array *MUST* match the order in parsec_datatype_basicDatatypes
 * (use of designated initializers should relax this restrictions some)
 */
PARSEC_DECLSPEC const size_t parsec_datatype_local_sizes[PARSEC_DATATYPE_MAX_PREDEFINED] =
{
    [PARSEC_DATATYPE_INT1] = sizeof(int8_t),
    [PARSEC_DATATYPE_INT2] = sizeof(int16_t),
    [PARSEC_DATATYPE_INT4] = sizeof(int32_t),
    [PARSEC_DATATYPE_INT8] = sizeof(int64_t),
    [PARSEC_DATATYPE_INT16] = 16,    /* sizeof (int128_t) */
    [PARSEC_DATATYPE_UINT1] = sizeof(uint8_t),
    [PARSEC_DATATYPE_UINT2] = sizeof(uint16_t),
    [PARSEC_DATATYPE_UINT4] = sizeof(uint32_t),
    [PARSEC_DATATYPE_UINT8] = sizeof(uint64_t),
    [PARSEC_DATATYPE_UINT16] = 16,    /* sizeof (uint128_t) */
    [PARSEC_DATATYPE_FLOAT2] = 2,     /* sizeof (float2) */
    [PARSEC_DATATYPE_FLOAT4] = 4,     /* sizeof (float4) */
    [PARSEC_DATATYPE_FLOAT8] = 8,     /* sizeof (float8) */
    [PARSEC_DATATYPE_FLOAT12] = 12,   /* sizeof (float12) */
    [PARSEC_DATATYPE_FLOAT16] = 16,   /* sizeof (float16) */
#if defined(HAVE_SHORT_FLOAT__COMPLEX)
    [PARSEC_DATATYPE_SHORT_FLOAT_COMPLEX] = sizeof(short float _Complex),
#elif defined(HAVE_PARSEC_SHORT_FLOAT_COMPLEX_T)
    [PARSEC_DATATYPE_SHORT_FLOAT_COMPLEX] = sizeof(parsec_short_float_complex_t),
#else
    [PARSEC_DATATYPE_SHORT_FLOAT_COMPLEX] = 4, /* typical sizeof(short float _Complex) */
#endif
    [PARSEC_DATATYPE_FLOAT_COMPLEX] = sizeof(float _Complex),
    [PARSEC_DATATYPE_DOUBLE_COMPLEX] = sizeof(double _Complex),
    [PARSEC_DATATYPE_LONG_DOUBLE_COMPLEX] = sizeof(long double _Complex),
    [PARSEC_DATATYPE_BOOL] = sizeof (_Bool),
    [PARSEC_DATATYPE_WCHAR] = sizeof (wchar_t),
};

/*
 * NOTE: The order of this array *MUST* match what is listed in datatype.h
 * (use of designated initializers should relax this restrictions some)
 */
PARSEC_DECLSPEC const parsec_datatype_s* parsec_datatype_basicDatatypes[PARSEC_DATATYPE_MAX_PREDEFINED] = {
    [PARSEC_DATATYPE_LOOP] = &parsec_datatype_loop,
    [PARSEC_DATATYPE_END_LOOP] = &parsec_datatype_end_loop,
    [PARSEC_DATATYPE_LB] = &parsec_datatype_lb,
    [PARSEC_DATATYPE_UB] = &parsec_datatype_ub,
    [PARSEC_DATATYPE_INT1] = &parsec_datatype_int1,
    [PARSEC_DATATYPE_INT2] = &parsec_datatype_int2,
    [PARSEC_DATATYPE_INT4] = &parsec_datatype_int4,
    [PARSEC_DATATYPE_INT8] = &parsec_datatype_int8,
    [PARSEC_DATATYPE_INT16] = &parsec_datatype_int16,       /* Yes, double-machine word integers are available */
    [PARSEC_DATATYPE_UINT1] = &parsec_datatype_uint1,
    [PARSEC_DATATYPE_UINT2] = &parsec_datatype_uint2,
    [PARSEC_DATATYPE_UINT4] = &parsec_datatype_uint4,
    [PARSEC_DATATYPE_UINT8] = &parsec_datatype_uint8,
    [PARSEC_DATATYPE_UINT16] = &parsec_datatype_uint16,      /* Yes, double-machine word integers are available */
    [PARSEC_DATATYPE_FLOAT2] = &parsec_datatype_float2,
    [PARSEC_DATATYPE_FLOAT4] = &parsec_datatype_float4,
    [PARSEC_DATATYPE_FLOAT8] = &parsec_datatype_float8,
    [PARSEC_DATATYPE_FLOAT12] = &parsec_datatype_float12,
    [PARSEC_DATATYPE_FLOAT16] = &parsec_datatype_float16,
    [PARSEC_DATATYPE_SHORT_FLOAT_COMPLEX] = &parsec_datatype_short_float_complex,
    [PARSEC_DATATYPE_FLOAT_COMPLEX] = &parsec_datatype_float_complex,
    [PARSEC_DATATYPE_DOUBLE_COMPLEX] = &parsec_datatype_double_complex,
    [PARSEC_DATATYPE_LONG_DOUBLE_COMPLEX] = &parsec_datatype_long_double_complex,
    [PARSEC_DATATYPE_BOOL] = &parsec_datatype_bool,
    [PARSEC_DATATYPE_WCHAR] = &parsec_datatype_wchar,
    [PARSEC_DATATYPE_UNAVAILABLE] = &parsec_datatype_unavailable,
};


int parsec_datatype_register_params(void)
{
#if PARSEC_ENABLE_DEBUG
    int ret;

    ret = mca_base_var_register ("parsec", "mpi", NULL, "ddt_unpack_debug",
                                 "Whether to output debugging information in the ddt unpack functions (nonzero = enabled)",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, PARSEC_INFO_LVL_3,
                                 MCA_BASE_VAR_SCOPE_LOCAL, &parsec_ddt_unpack_debug);
    if (0 > ret) {
        return ret;
    }

    ret = mca_base_var_register ("parsec", "mpi", NULL, "ddt_pack_debug",
                                 "Whether to output debugging information in the ddt pack functions (nonzero = enabled)",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, PARSEC_INFO_LVL_3,
                                 MCA_BASE_VAR_SCOPE_LOCAL, &parsec_ddt_pack_debug);
    if (0 > ret) {
        return ret;
    }

    ret = mca_base_var_register ("parsec", "mpi", NULL, "ddt_raw_debug",
                                 "Whether to output debugging information in the ddt raw functions (nonzero = enabled)",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, PARSEC_INFO_LVL_3,
                                 MCA_BASE_VAR_SCOPE_LOCAL, &parsec_ddt_raw_debug);
    if (0 > ret) {
        return ret;
    }

    ret = mca_base_var_register ("parsec", "mpi", NULL, "ddt_position_debug",
                                 "Non zero lead to output generated by the datatype position functions",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, PARSEC_INFO_LVL_3,
                                 MCA_BASE_VAR_SCOPE_LOCAL, &parsec_ddt_position_debug);
    if (0 > ret) {
        return ret;
    }

    ret = mca_base_var_register ("parsec", "mpi", NULL, "ddt_copy_debug",
                                 "Whether to output debugging information in the ddt copy functions (nonzero = enabled)",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, PARSEC_INFO_LVL_3,
                                 MCA_BASE_VAR_SCOPE_LOCAL, &parsec_ddt_copy_debug);
    if (0 > ret) {
        return ret;
    }

    ret = mca_base_var_register ("parsec", "parsec", NULL, "ddt_verbose",
                                 "Set level of parsec datatype verbosity",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                 PARSEC_INFO_LVL_8, MCA_BASE_VAR_SCOPE_LOCAL,
                                 &parsec_ddt_verbose);
    if (0 > ret) {
        return ret;
    }
#endif /* PARSEC_ENABLE_DEBUG */

    return PARSEC_SUCCESS;
}

static void parsec_datatype_finalize (void)
{
    /* As the synonyms are just copies of the internal data we should not free them.
     * Anyway they are over the limit of PARSEC_DATATYPE_MAX_PREDEFINED so they will never get freed.
     */

    /* As they are statically allocated they cannot be released. But we
     * can call PARSEC_OBJ_DESTRUCT, just to free all internally allocated ressources.
     */
    /* clear all master convertors */
    parsec_convertor_destroy_masters();

    parsec_output_close (parsec_datatype_dfd);
    parsec_datatype_dfd = -1;
}

int32_t parsec_datatype_init( void )
{
    const parsec_datatype_s* datatype;
    int32_t i;

    /**
     * Force he initialization of the parsec_datatype_s class. This will allow us to
     * call PARSEC_OBJ_DESTRUCT without going too deep in the initialization process.
     */
    parsec_class_initialize(PARSEC_OBJ_CLASS(parsec_datatype_s));
    for( i = PARSEC_DATATYPE_FIRST_TYPE; i < PARSEC_DATATYPE_MAX_PREDEFINED; i++ ) {
        datatype = parsec_datatype_basicDatatypes[i];

        /* All of the predefined PARSEC types don't have any GAPS! */
        datatype->desc.desc[0].elem.common.flags = PARSEC_DATATYPE_FLAG_PREDEFINED |
                                                   PARSEC_DATATYPE_FLAG_DATA |
                                                   PARSEC_DATATYPE_FLAG_CONTIGUOUS |
                                                   PARSEC_DATATYPE_FLAG_NO_GAPS;
        datatype->desc.desc[0].elem.common.type  = i;
        datatype->desc.desc[0].elem.count        = 1;
        datatype->desc.desc[0].elem.blocklen     = 1;
        datatype->desc.desc[0].elem.disp         = 0;
        datatype->desc.desc[0].elem.extent       = datatype->size;

        datatype->desc.desc[1].end_loop.common.flags    = 0;
        datatype->desc.desc[1].end_loop.common.type     = PARSEC_DATATYPE_END_LOOP;
        datatype->desc.desc[1].end_loop.items           = 1;
        datatype->desc.desc[1].end_loop.first_elem_disp = datatype->desc.desc[0].elem.disp;
        datatype->desc.desc[1].end_loop.size            = datatype->size;
    }

    /* Enable a private output stream for datatype */
    if( parsec_ddt_verbose > 0 ) {
        parsec_datatype_dfd = parsec_output_open(NULL);
        parsec_output_set_verbosity(parsec_datatype_dfd, parsec_ddt_verbose);
    }

    parsec_finalize_register_cleanup (parsec_datatype_finalize);

    return PARSEC_SUCCESS;
}

#if PARSEC_ENABLE_DEBUG
/*
 * Set a breakpoint to this function in your favorite debugger
 * to make it stop on all pack and unpack errors.
 */
int parsec_datatype_safeguard_pointer_debug_breakpoint( const void* actual_ptr, int length,
                                                      const void* initial_ptr,
                                                      const parsec_datatype_s* pData,
                                                      int count )
{
    return 0;
}
#endif  /* PARSEC_ENABLE_DEBUG */
