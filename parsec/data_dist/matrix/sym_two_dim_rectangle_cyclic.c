/*
 * Copyright (c) 2009-2020 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#include "parsec/parsec_config.h"
#include "parsec/parsec_internal.h"
#include "parsec/utils/debug.h"

#include <stdlib.h>
#include <stdio.h>
#ifdef PARSEC_HAVE_STRING_H
#include <string.h>
#endif
#ifdef PARSEC_HAVE_LIMITS_H
#include <limits.h>
#endif
#include <assert.h>
#ifdef PARSEC_HAVE_ERRNO_H
#include <errno.h>
#endif
#ifdef PARSEC_HAVE_STDARG_H
#include <stdarg.h>
#endif
#include <stdint.h>
#include <inttypes.h>
#include <math.h>

#include <assert.h>
#include "parsec/data_dist/matrix/matrix.h"
#include "parsec/data_dist/matrix/sym_two_dim_rectangle_cyclic.h"
#include "parsec/mca/device/device.h"
#include "parsec/vpmap.h"
#include "parsec/data.h"

#if !defined(UINT_MAX)
#define UINT_MAX (~0UL)
#endif

static uint32_t sym_twoDBC_rank_of(parsec_data_collection_t * desc, ...);
static int32_t sym_twoDBC_vpid_of(parsec_data_collection_t *desc, ...);
static parsec_data_t* sym_twoDBC_data_of(parsec_data_collection_t *desc, ...);
static uint32_t sym_twoDBC_rank_of_key(parsec_data_collection_t *desc, parsec_data_key_t key);
static int32_t sym_twoDBC_vpid_of_key(parsec_data_collection_t *desc, parsec_data_key_t key);
static parsec_data_t* sym_twoDBC_data_of_key(parsec_data_collection_t *desc, parsec_data_key_t key);

static uint32_t sym_twoDBC_kview_rank_of(parsec_data_collection_t* dc, ...);
static int32_t sym_twoDBC_kview_vpid_of(parsec_data_collection_t* dc, ...);
static parsec_data_t* sym_twoDBC_kview_data_of(parsec_data_collection_t* dc, ...);
static uint32_t sym_twoDBC_kview_rank_of_key(parsec_data_collection_t* dc, parsec_data_key_t key);
static int32_t sym_twoDBC_kview_vpid_of_key(parsec_data_collection_t* dc, parsec_data_key_t key);
static parsec_data_t* sym_twoDBC_kview_data_of_key(parsec_data_collection_t* dc, parsec_data_key_t key);

#if !PARSEC_KCYCLIC_WITH_VIEW
static uint32_t sym_twoDBC_kcyclic_rank_of(parsec_data_collection_t* dc, ...);
static int32_t sym_twoDBC_kcyclic_vpid_of(parsec_data_collection_t* dc, ...);
static parsec_data_t* sym_twoDBC_kcyclic_data_of(parsec_data_collection_t* dc, ...);
static uint32_t sym_twoDBC_kcyclic_rank_of_key(parsec_data_collection_t* dc, parsec_data_key_t key);
static int32_t sym_twoDBC_kcyclic_vpid_of_key(parsec_data_collection_t* dc, parsec_data_key_t key);
static parsec_data_t* sym_twoDBC_kcyclic_data_of_key(parsec_data_collection_t* dc, parsec_data_key_t key);
#endif

static int sym_twoDBC_memory_register(parsec_data_collection_t* desc, parsec_device_module_t* device)
{
    sym_two_dim_block_cyclic_t * sym_twodbc = (sym_two_dim_block_cyclic_t *)desc;
    if( NULL == sym_twodbc->mat ) {
        return PARSEC_SUCCESS;
    }
    return device->memory_register(device, desc,
                                   sym_twodbc->mat,
                                   ((size_t)sym_twodbc->super.nb_local_tiles * (size_t)sym_twodbc->super.bsiz *
                                   (size_t)parsec_datadist_getsizeoftype(sym_twodbc->super.mtype)));
}

static int sym_twoDBC_memory_unregister(parsec_data_collection_t* desc, parsec_device_module_t* device)
{
    sym_two_dim_block_cyclic_t * sym_twodbc = (sym_two_dim_block_cyclic_t *)desc;
    if( NULL == sym_twodbc->mat ) {
        return PARSEC_SUCCESS;
    }
    return device->memory_unregister(device, desc, sym_twodbc->mat);
}

void sym_two_dim_block_cyclic_init(sym_two_dim_block_cyclic_t * dc,
                                   enum matrix_type mtype,
                                   int nodes, int myrank,
                                   int mb,   int nb,   /* Tile size */
                                   int lm,   int ln,   /* Global matrix size (what is stored)*/
                                   int i,    int j,    /* Staring point in the global matrix */
                                   int m,    int n,    /* Submatrix size (the one concerned by the computation */
                                   int kp,   int kq,   /* k-cyclicity */
                                   int P, int uplo )
{
    int total;
    int Q;
    /* Initialize the tiled_matrix descriptor */
    parsec_data_collection_t *o = &(dc->super.super);

    parsec_tiled_matrix_dc_init( &(dc->super), mtype, matrix_Tile,
                            sym_two_dim_block_cyclic_type,
                            nodes, myrank,
                            mb, nb, lm, ln, i, j, m, n );
    dc->mat = NULL;  /* No data associated with the matrix yet */

    /* set the methods */
    if( (kp == 1) && (kq == 1) ) {
        o->rank_of     = sym_twoDBC_rank_of;
        o->rank_of_key = sym_twoDBC_rank_of_key;
        o->vpid_of     = sym_twoDBC_vpid_of;
        o->vpid_of_key = sym_twoDBC_vpid_of_key;
        o->data_of     = sym_twoDBC_data_of;
        o->data_of_key = sym_twoDBC_data_of_key;
    } else {
#if !PARSEC_KCYCLIC_WITH_VIEW
        o->rank_of      = sym_twoDBC_kcyclic_rank_of;
        o->vpid_of      = sym_twoDBC_kcyclic_vpid_of;
        o->data_of      = sym_twoDBC_kcyclic_data_of;
        o->rank_of_key  = sym_twoDBC_kcyclic_rank_of_key;
        o->vpid_of_key  = sym_twoDBC_kcyclic_vpid_of_key;
        o->data_of_key  = sym_twoDBC_kcyclic_data_of_key;
#else
        sym_two_dim_block_cyclic_kview(dc, dc, kp, kq);
#endif /* PARSEC_KCYCLIC_WITH_VIEW */
    }

    o->register_memory   = sym_twoDBC_memory_register;
    o->unregister_memory = sym_twoDBC_memory_unregister;

    if(nodes < P) {
        parsec_warning("Block Cyclic Distribution:\tThere are not enough nodes (%d) to make a process grid with P=%d", nodes, P);
        P = nodes;
    }
    Q = nodes / P;
    if(nodes != P*Q)
        parsec_warning("Block Cyclic Distribution:\tNumber of nodes %d doesn't match the process grid %dx%d", nodes, P, Q);

#if !PARSEC_KCYCLIC_WITH_VIEW
    grid_2Dcyclic_init(&dc->grid, myrank, P, Q, kp, kq);
#else
    grid_2Dcyclic_init(&dc->grid, myrank, P, Q, 1, 1);
#endif /* PARSEC_KCYCLIC_WITH_VIEW */

    /* Extra parameters */
    dc->uplo = uplo;

    /* find the number of tiles this process will handle */
    total = 0; /* number of tiles handled by the process */
    if( uplo == matrix_Lower ) {
        int row = (dc->grid.rrank) * (dc->grid.krows); /* tile row considered */
        /* for each row of tiles, compute number of tiles in that row */
        while( row < dc->super.lmt ) {
            int krow = 0;
            /* iterate by k-row */
            while( (krow < dc->grid.krows) && ((row + krow) < dc->super.lmt) ) {
                int nb_tile_total  = row + krow + 1;                   /* nb of total tiles in this row */
                int nb_ktile_total = nb_tile_total / (dc->grid.kcols); /* nb of full k-tiles in this row */
                int nb_tile_extra  = nb_tile_total % (dc->grid.kcols); /* nb of extra tiles in this row */
                int nb_ktile_proc  = nb_ktile_total / (dc->grid.cols); /* nb of k-tiles per process in this row */
                int nb_ktile_extra = nb_ktile_total % (dc->grid.cols); /* nb of extra k-tiles in this row */

                int nb_tile = nb_ktile_proc * (dc->grid.kcols);        /* nb of local tiles in this row */
                /* add extra k-tile */
                if( nb_ktile_extra > dc->grid.crank )
                    nb_tile += dc->grid.kcols;
                /* add extra tiles */
                else if( nb_ktile_extra == dc->grid.crank )
                    nb_tile += nb_tile_extra;

                total += nb_tile;
                krow++;
            }
            row += (dc->grid.rows) * (dc->grid.krows);
        }
    } else { /* Upper */
        int col = (dc->grid.crank) * (dc->grid.kcols); /* tile column considered */
        /* for each column of tiles, compute number of tiles in that column */
        while( col < dc->super.lnt ) {
            int kcol = 0;
            /* iterate by k-col */
            while( (kcol < dc->grid.kcols) && ((col + kcol) < dc->super.lnt) ) {
                int nb_tile_total  = col + kcol + 1;                   /* nb of total tiles in this column */
                int nb_ktile_total = nb_tile_total / (dc->grid.krows); /* nb of full k-tiles in this column */
                int nb_tile_extra  = nb_tile_total % (dc->grid.krows); /* nb of extra tiles in this column */
                int nb_ktile_proc  = nb_ktile_total / (dc->grid.rows); /* nb of k-tiles per process in this column */
                int nb_ktile_extra = nb_ktile_total % (dc->grid.rows); /* nb of extra k-tiles in this column */

                int nb_tile = nb_ktile_proc * (dc->grid.krows);        /* nb of tiles in this column */
                /* add extra k-tile */
                if( nb_ktile_extra > dc->grid.rrank )
                    nb_tile += dc->grid.krows;
                /* add extra tiles */
                else if( nb_ktile_extra == dc->grid.rrank )
                    nb_tile += nb_tile_extra;

                total += nb_tile;
                kcol++;
            }
            col += (dc->grid.cols) * (dc->grid.kcols);
        }
    }

    dc->super.nb_local_tiles = total;
    dc->super.data_map = (parsec_data_t**)calloc(dc->super.nb_local_tiles, sizeof(parsec_data_t*));
}

static uint32_t sym_twoDBC_rank_of(parsec_data_collection_t * desc, ...)
{
    int cr, m, n;
    int rr;
    int res;
    va_list ap;
    sym_two_dim_block_cyclic_t * dc;
    dc = (sym_two_dim_block_cyclic_t *)desc;

    /* Get coordinates */
    va_start(ap, desc);
    m = va_arg(ap, unsigned int);
    n = va_arg(ap, unsigned int);
    va_end(ap);

    /* Offset by (i,j) to translate (m,n) in the global matrix */
    m += dc->super.i / dc->super.mb;
    n += dc->super.j / dc->super.nb;

    assert( m < dc->super.mt );
    assert( n < dc->super.nt );

    assert( (dc->uplo == matrix_Lower && m>=n) ||
            (dc->uplo == matrix_Upper && n>=m) );
    if ( ((dc->uplo == matrix_Lower) && (m < n)) ||
         ((dc->uplo == matrix_Upper) && (m > n)) )
    {
        return UINT_MAX;
    }

    /* for tile (m,n), first find coordinate of process in
     process grid which possess the tile in block cyclic dist */
    rr = m % dc->grid.rows;
    cr = n % dc->grid.cols;

    /* P(rr, cr) has the tile, compute the mpi rank*/
    res = rr * dc->grid.cols + cr;

    return res;
}

static void sym_twoDBC_key_to_coordinates(parsec_data_collection_t *desc, parsec_data_key_t key, int *m, int *n)
{
    int _m, _n;
    parsec_tiled_matrix_dc_t * dc;

    dc = (parsec_tiled_matrix_dc_t *)desc;

    _m = key % dc->lmt;
    _n = key / dc->lmt;
    *m = _m - dc->i / dc->mb;
    *n = _n - dc->j / dc->nb;
}

static uint32_t sym_twoDBC_rank_of_key(parsec_data_collection_t *desc, parsec_data_key_t key)
{
    int m, n;
    sym_twoDBC_key_to_coordinates(desc, key, &m, &n);
    return sym_twoDBC_rank_of(desc, m, n);
}

static parsec_data_t* sym_twoDBC_data_of(parsec_data_collection_t *desc, ...)
{
    int m, n, position;
    sym_two_dim_block_cyclic_t * dc;
    size_t pos = 0;
    va_list ap;

    dc = (sym_two_dim_block_cyclic_t *)desc;

    /* Get coordinates */
    va_start(ap, desc);
    m = (int)va_arg(ap, unsigned int);
    n = (int)va_arg(ap, unsigned int);
    va_end(ap);

    /* Offset by (i,j) to translate (m,n) in the global matrix */
    m += dc->super.i / dc->super.mb;
    n += dc->super.j / dc->super.nb;

    assert( m < dc->super.mt );
    assert( n < dc->super.nt );

#if defined(DISTRIBUTED)
    assert(desc->myrank == sym_twoDBC_rank_of(desc, m, n));
#endif
    assert( dc->super.storage == matrix_Tile );
    assert( (dc->uplo == matrix_Lower && m>=n) ||
            (dc->uplo == matrix_Upper && n>=m) );

    position = sym_twoDBC_coordinates_to_position(dc, m, n);

    /* If mat allocatd, set pos to the right position for each tile */
    if( NULL != dc->mat )
        pos = position;

    return parsec_matrix_create_data( &dc->super,
                                     (char*)dc->mat + pos * dc->super.bsiz * parsec_datadist_getsizeoftype(dc->super.mtype),
                                     position, (n * dc->super.lmt) + m );
}

static parsec_data_t* sym_twoDBC_data_of_key(parsec_data_collection_t *desc, parsec_data_key_t key)
{
    int m, n;
    sym_twoDBC_key_to_coordinates(desc, key, &m, &n);
    return sym_twoDBC_data_of(desc, m, n);
}

static int32_t sym_twoDBC_vpid_of(parsec_data_collection_t *desc, ...)
{
    int m, n, p, q, pq;
    int local_m, local_n;
    sym_two_dim_block_cyclic_t * dc;
    va_list ap;
    int32_t vpid;
    dc = (sym_two_dim_block_cyclic_t *)desc;

    pq = vpmap_get_nb_vp();
    if ( pq == 1 )
        return 0;

    q = dc->grid.vp_q;
    p = dc->grid.vp_p;
    assert(p*q == pq);


    /* Get coordinates */
    va_start(ap, desc);
    m = (int)va_arg(ap, unsigned int);
    n = (int)va_arg(ap, unsigned int);
    va_end(ap);

    /* Offset by (i,j) to translate (m,n) in the global matrix */
    m += dc->super.i / dc->super.mb;
    n += dc->super.j / dc->super.nb;

    assert( m < dc->super.mt );
    assert( n < dc->super.nt );

#if defined(DISTRIBUTED)
    assert(desc->myrank == sym_twoDBC_rank_of(desc, m, n));
#endif
    assert( (dc->uplo == matrix_Lower && m>=n) ||
            (dc->uplo == matrix_Upper && n>=m) );

    /* Compute the local tile row */
    local_m = m / dc->grid.rows;
    assert( (m % dc->grid.rows) == dc->grid.rrank );

    /* Compute the local column */
    local_n = n / dc->grid.cols;
    assert( (n % dc->grid.cols) == dc->grid.crank );

    vpid = (local_n % q) * p + (local_m % p);
    assert( vpid < vpmap_get_nb_vp() );
    return vpid;
}

static int32_t sym_twoDBC_vpid_of_key(parsec_data_collection_t *desc, parsec_data_key_t key)
{
    int m, n;
    sym_twoDBC_key_to_coordinates(desc, key, &m, &n);
    return sym_twoDBC_vpid_of(desc, m, n);
}

/****
 * Set of functions with a pseudo k-cyclic view of the distribution
 ****/

void sym_two_dim_block_cyclic_kview( sym_two_dim_block_cyclic_t* target,
                                     sym_two_dim_block_cyclic_t* origin,
                                     int kp, int kq )
{
    assert( (origin->grid.krows == 1) && (origin->grid.kcols == 1) );
    *target = *origin;
    target->grid.krows = kp;
    target->grid.kcols = kq;
    target->super.super.rank_of     = sym_twoDBC_kview_rank_of;
    target->super.super.data_of     = sym_twoDBC_kview_data_of;
    target->super.super.vpid_of     = sym_twoDBC_kview_vpid_of;
    target->super.super.rank_of_key = sym_twoDBC_kview_rank_of_key;
    target->super.super.data_of_key = sym_twoDBC_kview_data_of_key;
    target->super.super.vpid_of_key = sym_twoDBC_kview_vpid_of_key;
}

static inline unsigned int kview_compute_m(sym_two_dim_block_cyclic_t* desc, unsigned int m)
{
    unsigned int p, ps, mt;
    p = desc->grid.rows;
    ps = desc->grid.krows;
    mt = desc->super.mt;
    do {
        m = m-m%(p*ps) + (m%ps)*p + (m/ps)%p;
    } while(m >= mt);
    return m;
}

static inline unsigned int kview_compute_n(sym_two_dim_block_cyclic_t* desc, unsigned int n)
{
    unsigned int q, qs, nt;
    q = desc->grid.cols;
    qs = desc->grid.kcols;
    nt = desc->super.nt;
    do {
        n = n-n%(q*qs) + (n%qs)*q + (n/qs)%q;
    } while(n >= nt);
    return n;
}

static uint32_t sym_twoDBC_kview_rank_of(parsec_data_collection_t* dc, ...)
{
    unsigned int m, n, sm, sn;
    sym_two_dim_block_cyclic_t* desc = (sym_two_dim_block_cyclic_t*)dc;
    va_list ap;
    va_start(ap, dc);
    m = va_arg(ap, unsigned int);
    n = va_arg(ap, unsigned int);
    va_end(ap);
    sm = kview_compute_m(desc, m);
    sn = kview_compute_n(desc, n);
    return sym_twoDBC_rank_of(dc, sm, sn);
}

static uint32_t sym_twoDBC_kview_rank_of_key(parsec_data_collection_t *desc, parsec_data_key_t key)
{
    int m, n;
    sym_twoDBC_key_to_coordinates(desc, key, &m, &n);
    return sym_twoDBC_kview_rank_of(desc, m, n);
}

static int32_t sym_twoDBC_kview_vpid_of(parsec_data_collection_t* dc, ...)
{
    unsigned int m, n;
    sym_two_dim_block_cyclic_t* desc = (sym_two_dim_block_cyclic_t*)dc;
    va_list ap;
    va_start(ap, dc);
    m = va_arg(ap, unsigned int);
    n = va_arg(ap, unsigned int);
    va_end(ap);
    m = kview_compute_m(desc, m);
    n = kview_compute_n(desc, n);
    return sym_twoDBC_vpid_of(dc, m, n);
}

static int32_t sym_twoDBC_kview_vpid_of_key(parsec_data_collection_t *desc, parsec_data_key_t key)
{
    int m, n;
    sym_twoDBC_key_to_coordinates(desc, key, &m, &n);
    return sym_twoDBC_kview_vpid_of(desc, m, n);
}

static parsec_data_t* sym_twoDBC_kview_data_of(parsec_data_collection_t* dc, ...)
{
    unsigned int m, n;
    sym_two_dim_block_cyclic_t* desc = (sym_two_dim_block_cyclic_t*)dc;
    va_list ap;
    va_start(ap, dc);
    m = va_arg(ap, unsigned int);
    n = va_arg(ap, unsigned int);
    va_end(ap);
    m = kview_compute_m(desc, m);
    n = kview_compute_n(desc, n);
    return sym_twoDBC_data_of(dc, m, n);
}

static parsec_data_t* sym_twoDBC_kview_data_of_key(parsec_data_collection_t *desc, parsec_data_key_t key)
{
    int m, n;
    sym_twoDBC_key_to_coordinates(desc, key, &m, &n);
    return sym_twoDBC_kview_data_of(desc, m, n);
}

#if !PARSEC_KCYCLIC_WITH_VIEW
/*
 *
 * Set of functions with k-cyclicity support
 *
 */
static uint32_t sym_twoDBC_kcyclic_rank_of(parsec_data_collection_t * desc, ...)
{
    unsigned int stc, cr, m, n;
    unsigned int str, rr;
    unsigned int res;
    va_list ap;
    sym_two_dim_block_cyclic_t * dc;
    dc = (sym_two_dim_block_cyclic_t *)desc;

    /* Get coordinates */
    va_start(ap, desc);
    m = va_arg(ap, unsigned int);
    n = va_arg(ap, unsigned int);
    va_end(ap);

    /* Offset by (i,j) to translate (m,n) in the global matrix */
    m += dc->super.i / dc->super.mb;
    n += dc->super.j / dc->super.nb;

    assert( m < dc->super.mt );
    assert( n < dc->super.nt );

    assert( (dc->uplo == matrix_Lower && m>=n) ||
            (dc->uplo == matrix_Upper && n>=m) );
    if ( ((dc->uplo == matrix_Lower) && (m < n)) ||
         ((dc->uplo == matrix_Upper) && (m > n)) )
    {
        return UINT_MAX;
    }

    /* for tile (m,n), first find coordinate of process in
       process grid which possess the tile in block cyclic dist */
    /* (m,n) is in k-cyclic tile (str, stc)*/
    str = m / dc->grid.krows;
    stc = n / dc->grid.kcols;

    /* P(rr, cr) has the tile, compute the mpi rank*/
    rr = str % dc->grid.rows;
    cr = stc % dc->grid.cols;
    res = rr * dc->grid.cols + cr;

    /* printf("tile (%d, %d) belongs to process %d [%d,%d] in a grid of %dx%d\n", */
    /*            m, n, res, rr, cr, dc->grid.rows, dc->grid.cols); */
    return res;
}

static uint32_t sym_twoDBC_kcyclic_rank_of_key(parsec_data_collection_t *desc, parsec_data_key_t key)
{
    int m, n;
    sym_twoDBC_key_to_coordinates(desc, key, &m, &n);
    return sym_twoDBC_kcyclic_rank_of(desc, m, n);
}

static int32_t sym_twoDBC_kcyclic_vpid_of(parsec_data_collection_t *desc, ...)
{
    int m, n, p, q, pq;
    int local_m, local_n;
    sym_two_dim_block_cyclic_t * dc;
    va_list ap;
    int32_t vpid;
    dc = (sym_two_dim_block_cyclic_t *)desc;

    /* If no vp, always return 0 */
    pq = vpmap_get_nb_vp();
    if ( pq == 1 )
        return 0;

    q = dc->grid.vp_q;
    p = dc->grid.vp_p;
    assert(p*q == pq);

    /* Get coordinates */
    va_start(ap, desc);
    m = (int)va_arg(ap, unsigned int);
    n = (int)va_arg(ap, unsigned int);
    va_end(ap);

    /* Offset by (i,j) to translate (m,n) in the global matrix */
    m += dc->super.i / dc->super.mb;
    n += dc->super.j / dc->super.nb;

    assert( m < dc->super.mt );
    assert( n < dc->super.nt );

#if defined(DISTRIBUTED)
    assert(desc->myrank == sym_twoDBC_kcyclic_rank_of(desc, m, n));
#endif
    assert( (dc->uplo == matrix_Lower && m>=n) ||
            (dc->uplo == matrix_Upper && n>=m) );

    /* Compute the local tile row */
    local_m = ( m / (dc->grid.krows * dc->grid.rows) ) * dc->grid.krows;
    m = m % (dc->grid.krows * dc->grid.rows);
    assert( m / dc->grid.krows == dc->grid.rrank);
    local_m += m % dc->grid.krows;

    /* Compute the local column */
    local_n = ( n / (dc->grid.kcols * dc->grid.cols) ) * dc->grid.kcols;
    n = n % (dc->grid.kcols * dc->grid.cols);
    assert( n / dc->grid.kcols == dc->grid.crank);
    local_n += n % dc->grid.kcols;

    vpid = (local_n % q) * p + (local_m % p);
    assert( vpid < vpmap_get_nb_vp() );
    return vpid;
}

static int32_t sym_twoDBC_kcyclic_vpid_of_key(parsec_data_collection_t *desc, parsec_data_key_t key)
{
    int m, n;
    sym_twoDBC_key_to_coordinates(desc, key, &m, &n);
    return sym_twoDBC_kcyclic_vpid_of(desc, m, n);
}

static parsec_data_t* sym_twoDBC_kcyclic_data_of(parsec_data_collection_t *desc, ...)
{
    size_t pos = 0;
    int m, n, local_m, local_n, position;
    va_list ap;
    sym_two_dim_block_cyclic_t * dc;
    dc = (sym_two_dim_block_cyclic_t *)desc;

    /* Get coordinates */
    va_start(ap, desc);
    m = (int)va_arg(ap, unsigned int);
    n = (int)va_arg(ap, unsigned int);
    va_end(ap);

    /* Offset by (i,j) to translate (m,n) in the global matrix */
    m += dc->super.i / dc->super.mb;
    n += dc->super.j / dc->super.nb;

    assert( m < dc->super.mt );
    assert( n < dc->super.nt );

#if defined(DISTRIBUTED)
    assert(desc->myrank == sym_twoDBC_kcyclic_rank_of(desc, m, n));
#endif
    assert( dc->super.storage == matrix_Tile );
    assert( (dc->uplo == matrix_Lower && m>=n) ||
            (dc->uplo == matrix_Upper && n>=m) );

    position = sym_twoDBC_kcyclic_coordinates_to_position(dc, m, n);

    /* If mat allocatd, set pos to the right position for each tile */
    if( NULL != dc->mat )
        pos = position;

    return parsec_matrix_create_data( &dc->super,
                                     (char*)dc->mat + pos * dc->super.bsiz * parsec_datadist_getsizeoftype(dc->super.mtype),
                                     position, (n * dc->super.lmt) + m );
}

static parsec_data_t* sym_twoDBC_kcyclic_data_of_key(parsec_data_collection_t *desc, parsec_data_key_t key)
{
    int m, n;
    sym_twoDBC_key_to_coordinates(desc, key, &m, &n);
    return sym_twoDBC_kcyclic_data_of(desc, m, n);
}

#endif /* PARSEC_KCYCLIC_WITH_VIEW */
