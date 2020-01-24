/*
 * Copyright (c) 2015-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */
#include <stddef.h>
#include <stdint.h>

#include <opal_config.h>
#include <opal/constants.h>
#include <opal/datatype/opal_datatype.h>
#include <opal/datatype/opal_datatype_internal.h>

#include "parsec/datatype.h"
#include "parsec/parsec_config.h"
#include "parsec/runtime.h"

#if !defined(PARSEC_OPAL_DATATYPES)
#error __FILE__ should only be used when Open PAL datatype support is enabled.
#endif  /* !defined(PARSEC_OPAL_DATATYPES) */

/* need to define own versions of datatypes
 * OPAL types are const and so cannot be used as parsec_datatype_t
 * without casting away the const, which is unsafe */

static opal_datatype_t parsec_datatype_null = OPAL_DATATYPE_INITIALIZER_EMPTY(OPAL_DATATYPE_FLAG_CONTIGUOUS);

#if   SIZEOF_INT == 1
static opal_datatype_t parsec_datatype_int = OPAL_DATATYPE_INITIALIZER_INT1(0);
#elif SIZEOF_INT == 2
static opal_datatype_t parsec_datatype_int = OPAL_DATATYPE_INITIALIZER_INT2(0);
#elif SIZEOF_INT == 4
static opal_datatype_t parsec_datatype_int = OPAL_DATATYPE_INITIALIZER_INT4(0);
#elif SIZEOF_INT == 8
static opal_datatype_t parsec_datatype_int = OPAL_DATATYPE_INITIALIZER_INT8(0);
#elif SIZEOF_INT == 16
static opal_datatype_t parsec_datatype_int = OPAL_DATATYPE_INITIALIZER_INT16(0);
#endif

static opal_datatype_t parsec_datatype_int8   = OPAL_DATATYPE_INITIALIZER_INT1(0);
static opal_datatype_t parsec_datatype_int16  = OPAL_DATATYPE_INITIALIZER_INT2(0);
static opal_datatype_t parsec_datatype_int32  = OPAL_DATATYPE_INITIALIZER_INT4(0);
static opal_datatype_t parsec_datatype_int64  = OPAL_DATATYPE_INITIALIZER_INT8(0);
static opal_datatype_t parsec_datatype_uint8  = OPAL_DATATYPE_INITIALIZER_UINT1(0);
static opal_datatype_t parsec_datatype_uint16 = OPAL_DATATYPE_INITIALIZER_UINT2(0);
static opal_datatype_t parsec_datatype_uint32 = OPAL_DATATYPE_INITIALIZER_UINT4(0);
static opal_datatype_t parsec_datatype_uint64 = OPAL_DATATYPE_INITIALIZER_UINT8(0);

#ifdef __STDC_IEC_559__
static opal_datatype_t parsec_datatype_float  = OPAL_DATATYPE_INITIALIZER_FLOAT4(0);
static opal_datatype_t parsec_datatype_double = OPAL_DATATYPE_INITIALIZER_FLOAT8(0);
#else
#  error "We assume that float == binary32 and double == binary64 (IEEE 754)"
#endif

#if   SIZEOF_LONG_DOUBLE == 8
static opal_datatype_t parsec_datatype_long_double = OPAL_DATATYPE_INITIALIZER_FLOAT8(0);
#elif SIZEOF_LONG_DOUBLE == 12
static opal_datatype_t parsec_datatype_long_double = OPAL_DATATYPE_INITIALIZER_FLOAT12(0);
#elif SIZEOF_LONG_DOUBLE == 16
static opal_datatype_t parsec_datatype_long_double = OPAL_DATATYPE_INITIALIZER_FLOAT16(0);
#endif

static opal_datatype_t parsec_datatype_complex        = OPAL_DATATYPE_INITIALIZER_FLOAT_COMPLEX(0);
static opal_datatype_t parsec_datatype_double_complex = OPAL_DATATYPE_INITIALIZER_DOUBLE_COMPLEX(0);

const parsec_datatype_t PARSEC_DATATYPE_NULL = &parsec_datatype_null;
const parsec_datatype_t parsec_datatype_int_t    = &parsec_datatype_int;
const parsec_datatype_t parsec_datatype_int8_t   = &parsec_datatype_int8;
const parsec_datatype_t parsec_datatype_int16_t  = &parsec_datatype_int16;
const parsec_datatype_t parsec_datatype_int32_t  = &parsec_datatype_int32;
const parsec_datatype_t parsec_datatype_int64_t  = &parsec_datatype_int64;
const parsec_datatype_t parsec_datatype_uint8_t  = &parsec_datatype_uint8;
const parsec_datatype_t parsec_datatype_uint16_t = &parsec_datatype_uint16;
const parsec_datatype_t parsec_datatype_uint32_t = &parsec_datatype_uint32;
const parsec_datatype_t parsec_datatype_uint64_t = &parsec_datatype_uint64;
const parsec_datatype_t parsec_datatype_float_t          = &parsec_datatype_float;
const parsec_datatype_t parsec_datatype_double_t         = &parsec_datatype_double;
const parsec_datatype_t parsec_datatype_long_double_t    = &parsec_datatype_long_double;
const parsec_datatype_t parsec_datatype_complex_t        = &parsec_datatype_complex;
const parsec_datatype_t parsec_datatype_double_complex_t = &parsec_datatype_double_complex;


/**
 * Map the datatype creation to the well designed and well known MPI datatype
 * mainipulation. However, right now we only provide the most basic types and
 * functions to mix them together.
 *
 * However, this file contains only the support functions needed when MPI is not
 * available.
 */

#define PARSEC_OPAL_RETURN(ret) (OPAL_SUCCESS == (ret)) ? PARSEC_SUCCESS : PARSEC_ERROR
#define PARSEC_OPAL_CONTINUE(ret) if (OPAL_SUCCESS != (ret)) return PARSEC_ERROR

static int parsec_type_duplicate(parsec_datatype_t oldType,
                                 parsec_datatype_t* newType)
{
    opal_datatype_t *type = opal_datatype_create(oldType->desc.used + 2);
    if (NULL == type) {
        return PARSEC_ERR_OUT_OF_RESOURCE;
    }
    opal_datatype_clone(oldType, type);
    *newType = type;
    return PARSEC_SUCCESS;
}

int parsec_type_size( parsec_datatype_t type,
                     int *size )
{
    size_t real_size;
    opal_datatype_type_size(type, &real_size);
    *size = real_size;
    return PARSEC_SUCCESS;
}

int parsec_type_extent(parsec_datatype_t type, ptrdiff_t* lb, ptrdiff_t* extent) {
    opal_datatype_get_extent(type, lb, extent);
    return PARSEC_SUCCESS;
}

int parsec_type_free(parsec_datatype_t* type) {
    int32_t ret = opal_datatype_destroy(type);
    return PARSEC_OPAL_RETURN(ret);
}

int parsec_type_create_contiguous(int count,
                                  parsec_datatype_t oldType,
                                  parsec_datatype_t* newType)
{
    int32_t ret = opal_datatype_create_contiguous(count, oldType, newType);
    PARSEC_OPAL_CONTINUE(ret);
    ret = opal_datatype_commit(*newType);
    return PARSEC_OPAL_RETURN(ret);
}

int parsec_type_create_vector(int count,
                              int bLength,
                              int stride,
                              parsec_datatype_t oldType,
                              parsec_datatype_t* newType)
{
    int32_t ret;
    opal_datatype_t *pTempData, *pData;
    ptrdiff_t extent = oldType->ub - oldType->lb;

    if ((0 == count) || (0 == bLength)) {
        return parsec_type_duplicate(PARSEC_DATATYPE_NULL, newType);
    }

    pData = opal_datatype_create(oldType->desc.used + 2);
    if ((bLength == stride) || (1 >= count)) {  /* the elements are contiguous */
        opal_datatype_add(pData, oldType, (size_t)count * bLength, 0, extent);
    } else if (1 == bLength) {
        opal_datatype_add(pData, oldType, count, 0, extent * stride);
    } else {
        opal_datatype_add(pData, oldType, bLength, 0, extent);
        pTempData = pData;
        pData = opal_datatype_create(oldType->desc.used + 2 + 2);
        opal_datatype_add(pData, pTempData, count, 0, extent * stride);
        OBJ_RELEASE(pTempData);
    }
    *newType = pData;
    ret = opal_datatype_commit(*newType);
    return PARSEC_OPAL_RETURN(ret);
}

int parsec_type_create_hvector(int count,
                               int bLength,
                               ptrdiff_t stride,
                               parsec_datatype_t oldType,
                               parsec_datatype_t* newType)
{
    int32_t ret;
    opal_datatype_t *pTempData, *pData;
    ptrdiff_t extent = oldType->ub - oldType->lb;

    if ((0 == count) || (0 == bLength)) {
        return parsec_type_duplicate(PARSEC_DATATYPE_NULL, newType);
    }

    pTempData = opal_datatype_create( oldType->desc.used + 2 );
    if (((extent * bLength) == stride) || (1 >= count)) {  /* contiguous */
        pData = pTempData;
        opal_datatype_add(pData, oldType, count * bLength, 0, extent);
    } else if (1 == bLength) {
        pData = pTempData;
        opal_datatype_add(pData, oldType, count, 0, stride);
    } else {
        opal_datatype_add(pTempData, oldType, bLength, 0, extent);
        pData = opal_datatype_create(oldType->desc.used + 2 + 2);
        opal_datatype_add(pData, pTempData, count, 0, stride);
        OBJ_RELEASE(pTempData);
    }
    *newType = pData;
    ret = opal_datatype_commit(*newType);
    return PARSEC_OPAL_RETURN(ret);
}

int parsec_type_create_indexed(int count,
                               const int pBlockLength[],
                               const int pDisp[],
                               parsec_datatype_t oldType,
                               parsec_datatype_t *newType)
{
    int32_t ret;
    ptrdiff_t extent, disp, endat;
    opal_datatype_t *pdt;
    size_t dLength;
    int i;

    /* ignore all cases that lead to an empty type */
    opal_datatype_type_size(oldType, &dLength);
    /* find first non zero */
    for (i = 0; (i < count) && (0 == pBlockLength[i]); i++)
        continue;
    if ((i == count) || (0 == dLength)) {
        return parsec_type_duplicate(PARSEC_DATATYPE_NULL, newType);
    }

    disp = pDisp[i];
    dLength = pBlockLength[i];
    endat = disp + dLength;
    opal_datatype_type_extent(oldType, &extent);

    pdt = opal_datatype_create((count - i) * (2 + oldType->desc.used));
    for (i += 1; i < count; i++) {
        if (0 == pBlockLength[i])  /* ignore empty length */
            continue;
        if (endat == pDisp[i]) { /* contiguous with the previsious */
            dLength += pBlockLength[i];
            endat += pBlockLength[i];
        } else {
            opal_datatype_add(pdt, oldType, dLength, disp * extent, extent);
            disp = pDisp[i];
            dLength = pBlockLength[i];
            endat = disp + pBlockLength[i];
        }
    }
    opal_datatype_add(pdt, oldType, dLength, disp * extent, extent);

    *newType = pdt;
    ret = opal_datatype_commit(*newType);
    return PARSEC_OPAL_RETURN(ret);
}

int parsec_type_create_indexed_block(int count,
                                    int bLength,
                                    const int pDisp[],
                                    parsec_datatype_t oldType,
                                    parsec_datatype_t *newType)
{
    int32_t ret;
    ptrdiff_t extent, disp, endat;
    opal_datatype_t *pdt;
    size_t dLength;
    int i;

    if ((count == 0) || (bLength == 0)) {
        return parsec_type_duplicate(PARSEC_DATATYPE_NULL, newType);
    }
    opal_datatype_type_extent(oldType, &extent);
    pdt = opal_datatype_create(count * (2 + oldType->desc.used));
    disp = pDisp[0];
    dLength = bLength;
    endat = disp + dLength;
    for (i = 1; i < count; i++) {
        if (endat == pDisp[i]) {
            /* contiguous with the previsious */
            dLength += bLength;
            endat += bLength;
        } else {
            opal_datatype_add(pdt, oldType, dLength, disp * extent, extent);
            disp = pDisp[i];
            dLength = bLength;
            endat = disp + bLength;
        }
    }
    opal_datatype_add(pdt, oldType, dLength, disp * extent, extent);

    *newType = pdt;
    ret = opal_datatype_commit(*newType);
    return PARSEC_OPAL_RETURN(ret);
}

int parsec_type_create_struct(int count,
                              const int pBlockLength[],
                              const ptrdiff_t pDisp[],
                              const parsec_datatype_t pTypes[],
                              parsec_datatype_t *newType)
{
    int32_t ret;
    ptrdiff_t disp = 0, endto, lastExtent, lastDisp;
    opal_datatype_t *pdt, *lastType;
    int lastBlock;
    int i, start_from;

    /* Find first non-zero length element */
    for (i = 0; (i < count) && (0 == pBlockLength[i]); i++)
        continue;
    if (i == count) {  /* either nothing or nothing relevant */
        return parsec_type_duplicate(PARSEC_DATATYPE_NULL, newType);
    }
    /* compute the total number of elements before we can
     * avoid increasing the size of the desc array often.
     */
    start_from = i;
    lastType = pTypes[start_from];
    lastBlock = pBlockLength[start_from];
    lastExtent = lastType->ub - lastType->lb;
    lastDisp = pDisp[start_from];
    endto = pDisp[start_from] + lastExtent * lastBlock;

    for (i = (start_from + 1); i < count; i++) {
        if ((pTypes[i] == lastType) && (pDisp[i] == endto)) {
            lastBlock += pBlockLength[i];
            endto = lastDisp + lastBlock * lastExtent;
        } else {
            disp += lastType->desc.used;
            if (lastBlock > 1)
                disp += 2;
            lastType = pTypes[i];
            lastExtent = lastType->ub - lastType->lb;
            lastBlock = pBlockLength[i];
            lastDisp = pDisp[i];
            endto = lastDisp + lastExtent * lastBlock;
        }
    }
    disp += lastType->desc.used;
    if (lastBlock != 1)
        disp += 2;

    lastType = pTypes[start_from];
    lastBlock = pBlockLength[start_from];
    lastExtent = lastType->ub - lastType->lb;
    lastDisp = pDisp[start_from];
    endto = pDisp[start_from] + lastExtent * lastBlock;

    pdt = opal_datatype_create((int32_t)disp);

    /* Do again the same loop but now add the elements */
    for (i = (start_from + 1); i < count; i++) {
        if ((pTypes[i] == lastType) && (pDisp[i] == endto)) {
            lastBlock += pBlockLength[i];
            endto = lastDisp + lastBlock * lastExtent;
        } else {
            opal_datatype_add(pdt, lastType, lastBlock, lastDisp, lastExtent);
            lastType = pTypes[i];
            lastExtent = lastType->ub - lastType->lb;
            lastBlock = pBlockLength[i];
            lastDisp = pDisp[i];
            endto = lastDisp + lastExtent * lastBlock;
        }
    }
    opal_datatype_add(pdt, lastType, lastBlock, lastDisp, lastExtent);

     *newType = pdt;
    ret = opal_datatype_commit(*newType);
    return PARSEC_OPAL_RETURN(ret);
}

int parsec_type_create_resized(parsec_datatype_t oldType,
                              ptrdiff_t lb,
                              ptrdiff_t extent,
                              parsec_datatype_t *newType)
{
    int32_t ret;
    opal_datatype_t *type;
    parsec_type_duplicate(oldType, &type);
    if (NULL == type) {
        return PARSEC_ERR_OUT_OF_RESOURCE;
    }
    opal_datatype_resize(type, lb, extent);
    *newType = type;
    ret = opal_datatype_commit(*newType);
    return PARSEC_OPAL_RETURN(ret);
}
