/*
 * Copyright (c) 2015-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */
#include <stddef.h>
#include <stdint.h>

#include "parsec/parsec_config.h"

#include "parsec/datatype/parsec_datatype.h"

#include "parsec/constants.h"
#include "parsec/datatype.h"

const parsec_datatype_t PARSEC_DATATYPE_NULL = &parsec_datatype_empty;

const parsec_datatype_t parsec_datatype_int_t =
#if   SIZEOF_INT == 1
                                              &parsec_datatype_int1;
#elif SIZEOF_INT == 2
                                              &parsec_datatype_int2;
#elif SIZEOF_INT == 4
                                              &parsec_datatype_int4;
#elif SIZOEF_INT == 8
                                              &parsec_datatype_int8;
#elif SIZEOF_INT == 16
                                              &parsec_datatype_int16;
#else
#  error "int larger than 16 bytes is unsupported"
#endif

const parsec_datatype_t parsec_datatype_int8_t  = &parsec_datatype_int1;
const parsec_datatype_t parsec_datatype_int16_t = &parsec_datatype_int2;
const parsec_datatype_t parsec_datatype_int32_t = &parsec_datatype_int4;
const parsec_datatype_t parsec_datatype_int64_t = &parsec_datatype_int8;

const parsec_datatype_t parsec_datatype_uint8_t  = &parsec_datatype_uint1;
const parsec_datatype_t parsec_datatype_uint16_t = &parsec_datatype_uint2;
const parsec_datatype_t parsec_datatype_uint32_t = &parsec_datatype_uint4;
const parsec_datatype_t parsec_datatype_uint64_t = &parsec_datatype_uint8;

#ifdef __STDC_IEC_559__
const parsec_datatype_t parsec_datatype_float_t  = &parsec_datatype_float4;
const parsec_datatype_t parsec_datatype_double_t = &parsec_datatype_float8;
#else
#  error "We assume that float == binary32 and double == binary64 (IEEE 754)"
#endif

const parsec_datatype_t parsec_datatype_long_double_t =
#if   SIZEOF_LONG_DOUBLE == 8
                                                      &parsec_datatype_float8;
#elif SIZEOF_LONG_DOUBLE == 12
                                                      &parsec_datatype_float12;
#elif SIZEOF_LONG_DOUBLE == 16
                                                      &parsec_datatype_float16;
#else
#  error "long double larger than 16 bytes is unsupported"
#endif

const parsec_datatype_t parsec_datatype_complex_t = &parsec_datatype_float_complex;
const parsec_datatype_t parsec_datatype_double_complex_t = &parsec_datatype_double_complex;

/**
 * Map the datatype creation to the well designed and well known MPI datatype
 * mainipulation. However, right now we only provide the most basic types and
 * functions to mix them together.
 *
 * However, this file contains only the support functions needed when MPI is not
 * available.
 */

static int parsec_type_duplicate(parsec_datatype_t oldType,
                                 parsec_datatype_t* newType)
{
    parsec_datatype_t type = parsec_datatype_create(oldType->desc.used + 2);
    if (NULL == type) {
        return PARSEC_ERR_OUT_OF_RESOURCE;
    }
    parsec_datatype_clone(oldType, type);
    *newType = type;
    return PARSEC_SUCCESS;
}

int parsec_type_size( parsec_datatype_t type,
                     int *size )
{
    size_t real_size;
    parsec_datatype_type_size(type, &real_size);
    *size = real_size;
    return PARSEC_SUCCESS;
}

int parsec_type_extent(parsec_datatype_t type, ptrdiff_t* lb, ptrdiff_t* extent) {
    parsec_datatype_get_extent(type, lb, extent);
    return PARSEC_SUCCESS;
}

int parsec_type_free(parsec_datatype_t* type) {
    return parsec_datatype_destroy(type);
}

int parsec_type_create_contiguous(int count,
                                  parsec_datatype_t oldType,
                                  parsec_datatype_t* newType)
{
    int32_t ret = parsec_datatype_create_contiguous(count, oldType, newType);
    if (PARSEC_SUCCESS != ret)
        return ret;
    ret = parsec_datatype_commit(*newType);
    return ret;
}

int parsec_type_create_vector(int count,
                              int bLength,
                              int stride,
                              parsec_datatype_t oldType,
                              parsec_datatype_t* newType)
{
    parsec_datatype_t pTempData, pData;
    ptrdiff_t extent = oldType->ub - oldType->lb;

    if ((0 == count) || (0 == bLength)) {
        return parsec_type_duplicate(PARSEC_DATATYPE_NULL, newType);
    }

    pData = parsec_datatype_create(oldType->desc.used + 2);
    if ((bLength == stride) || (1 >= count)) {  /* the elements are contiguous */
        parsec_datatype_add(pData, oldType, (size_t)count * bLength, 0, extent);
    } else if (1 == bLength) {
        parsec_datatype_add(pData, oldType, count, 0, extent * stride);
    } else {
        parsec_datatype_add(pData, oldType, bLength, 0, extent);
        pTempData = pData;
        pData = parsec_datatype_create(oldType->desc.used + 2 + 2);
        parsec_datatype_add(pData, pTempData, count, 0, extent * stride);
        PARSEC_OBJ_RELEASE(pTempData);
    }
    *newType = pData;
    return parsec_datatype_commit(*newType);
}

int parsec_type_create_hvector(int count,
                               int bLength,
                               ptrdiff_t stride,
                               parsec_datatype_t oldType,
                               parsec_datatype_t* newType)
{
    parsec_datatype_t pTempData, pData;
    ptrdiff_t extent = oldType->ub - oldType->lb;

    if ((0 == count) || (0 == bLength)) {
        return parsec_type_duplicate(PARSEC_DATATYPE_NULL, newType);
    }

    pTempData = parsec_datatype_create( oldType->desc.used + 2 );
    if (((extent * bLength) == stride) || (1 >= count)) {  /* contiguous */
        pData = pTempData;
        parsec_datatype_add(pData, oldType, count * bLength, 0, extent);
    } else if (1 == bLength) {
        pData = pTempData;
        parsec_datatype_add(pData, oldType, count, 0, stride);
    } else {
        parsec_datatype_add(pTempData, oldType, bLength, 0, extent);
        pData = parsec_datatype_create(oldType->desc.used + 2 + 2);
        parsec_datatype_add(pData, pTempData, count, 0, stride);
        PARSEC_OBJ_RELEASE(pTempData);
    }
    *newType = pData;
    return parsec_datatype_commit(*newType);
}

int parsec_type_create_indexed(int count,
                               const int pBlockLength[],
                               const int pDisp[],
                               parsec_datatype_t oldType,
                               parsec_datatype_t *newType)
{
    ptrdiff_t extent, disp, endat;
    parsec_datatype_t pdt;
    size_t dLength;
    int i;

    /* ignore all cases that lead to an empty type */
    parsec_datatype_type_size(oldType, &dLength);
    /* find first non zero */
    for (i = 0; (i < count) && (0 == pBlockLength[i]); i++)
        continue;
    if ((i == count) || (0 == dLength)) {
        return parsec_type_duplicate(PARSEC_DATATYPE_NULL, newType);
    }

    disp = pDisp[i];
    dLength = pBlockLength[i];
    endat = disp + dLength;
    parsec_datatype_type_extent(oldType, &extent);

    pdt = parsec_datatype_create((count - i) * (2 + oldType->desc.used));
    for (i += 1; i < count; i++) {
        if (0 == pBlockLength[i])  /* ignore empty length */
            continue;
        if (endat == pDisp[i]) { /* contiguous with the previsious */
            dLength += pBlockLength[i];
            endat += pBlockLength[i];
        } else {
            parsec_datatype_add(pdt, oldType, dLength, disp * extent, extent);
            disp = pDisp[i];
            dLength = pBlockLength[i];
            endat = disp + pBlockLength[i];
        }
    }
    parsec_datatype_add(pdt, oldType, dLength, disp * extent, extent);

    *newType = pdt;
    return parsec_datatype_commit(*newType);
}

int parsec_type_create_indexed_block(int count,
                                    int bLength,
                                    const int pDisp[],
                                    parsec_datatype_t oldType,
                                    parsec_datatype_t *newType)
{
    ptrdiff_t extent, disp, endat;
    parsec_datatype_t pdt;
    size_t dLength;
    int i;

    if ((count == 0) || (bLength == 0)) {
        return parsec_type_duplicate(PARSEC_DATATYPE_NULL, newType);
    }
    parsec_datatype_type_extent(oldType, &extent);
    pdt = parsec_datatype_create(count * (2 + oldType->desc.used));
    disp = pDisp[0];
    dLength = bLength;
    endat = disp + dLength;
    for (i = 1; i < count; i++) {
        if (endat == pDisp[i]) {
            /* contiguous with the previsious */
            dLength += bLength;
            endat += bLength;
        } else {
            parsec_datatype_add(pdt, oldType, dLength, disp * extent, extent);
            disp = pDisp[i];
            dLength = bLength;
            endat = disp + bLength;
        }
    }
    parsec_datatype_add(pdt, oldType, dLength, disp * extent, extent);

    *newType = pdt;
    return parsec_datatype_commit(*newType);
}

int parsec_type_create_struct(int count,
                              const int pBlockLength[],
                              const ptrdiff_t pDisp[],
                              const parsec_datatype_t pTypes[],
                              parsec_datatype_t *newType)
{
    ptrdiff_t disp = 0, endto, lastExtent, lastDisp;
    parsec_datatype_t pdt, lastType;
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

    pdt = parsec_datatype_create((int32_t)disp);

    /* Do again the same loop but now add the elements */
    for (i = (start_from + 1); i < count; i++) {
        if ((pTypes[i] == lastType) && (pDisp[i] == endto)) {
            lastBlock += pBlockLength[i];
            endto = lastDisp + lastBlock * lastExtent;
        } else {
            parsec_datatype_add(pdt, lastType, lastBlock, lastDisp, lastExtent);
            lastType = pTypes[i];
            lastExtent = lastType->ub - lastType->lb;
            lastBlock = pBlockLength[i];
            lastDisp = pDisp[i];
            endto = lastDisp + lastExtent * lastBlock;
        }
    }
    parsec_datatype_add(pdt, lastType, lastBlock, lastDisp, lastExtent);

    *newType = pdt;
    return parsec_datatype_commit(*newType);
}

int parsec_type_create_resized(parsec_datatype_t oldType,
                              ptrdiff_t lb,
                              ptrdiff_t extent,
                              parsec_datatype_t *newType)
{
    parsec_datatype_t type;
    parsec_type_duplicate(oldType, &type);
    if (NULL == type) {
        return PARSEC_ERR_OUT_OF_RESOURCE;
    }
    parsec_datatype_resize(type, lb, extent);
    *newType = type;
    return parsec_datatype_commit(*newType);
}
