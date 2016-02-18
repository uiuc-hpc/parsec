/*
 * Copyright (c) 2010-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */
/************************************************************
 *distributed matrix generation
 ************************************************************/

#include "dague_config.h"
#include "dague/dague_internal.h"
#include "dague/data.h"
#include "dague/data_distribution.h"
#include "data_dist/matrix/two_dim_rectangle_cyclic.h"
#include "data_dist/matrix/sym_two_dim_rectangle_cyclic.h"
#include "matrix.h"

#if defined(DAGUE_HAVE_MPI)
#include <mpi.h>
#endif

static uint32_t tiled_matrix_data_key(struct dague_ddesc_s *desc, ...);

#if defined(DAGUE_PROF_TRACE)
static int      tiled_matrix_key_to_string(struct dague_ddesc_s * desc, uint32_t datakey, char * buffer, uint32_t buffer_size);
#endif

dague_data_t*
dague_matrix_create_data(tiled_matrix_desc_t* matrix,
                         void* ptr,
                         int pos,
                         dague_data_key_t key)
{
    assert( pos <= matrix->nb_local_tiles );
    return dague_data_create( matrix->data_map + pos,
                              &(matrix->super), key, ptr,
                              matrix->bsiz * dague_datadist_getsizeoftype(matrix->mtype) );
}

void
dague_matrix_destroy_data( tiled_matrix_desc_t* matrix )
{
    if ( matrix->data_map != NULL ) {
        dague_data_t **data = matrix->data_map;
        int i;

        for(i=0; i<matrix->nb_local_tiles; i++, data++)
        {
            dague_data_destroy( *data );
        }

        free( matrix->data_map );
        matrix->data_map = NULL;
    }
    return;
}

dague_data_t*
fake_data_of(dague_ddesc_t *mat, ...)
{
    return dague_matrix_create_data( (tiled_matrix_desc_t*)mat, NULL,
                                     0, 0 );
}

/***************************************************************************//**
 *  Internal static descriptor initializer (PLASMA code)
 **/
void tiled_matrix_desc_init( tiled_matrix_desc_t *tdesc,
                             enum matrix_type    mtyp,
                             enum matrix_storage storage,
                             int dtype, int nodes, int myrank,
                             int mb, int nb,
                             int lm, int ln,
                             int i,  int j,
                             int m,  int n)
{
    dague_ddesc_t *o = (dague_ddesc_t*)tdesc;

    /* Super setup */
    dague_ddesc_init( o, nodes, myrank );

    /* Change the common data_key */
    o->data_key = tiled_matrix_data_key;

    /**
     * Setup the tiled matrix properties
     */

    /* Matrix address */
    /* tdesc->mat = NULL; */
    /* tdesc->A21 = (lm - lm%mb)*(ln - ln%nb); */
    /* tdesc->A12 = (     lm%mb)*(ln - ln%nb) + tdesc->A21; */
    /* tdesc->A22 = (lm - lm%mb)*(     ln%nb) + tdesc->A12; */

    /* Matrix properties */
    tdesc->data_map = NULL;
    tdesc->mtype    = mtyp;
    tdesc->storage  = storage;
    tdesc->dtype    = tiled_matrix_desc_type | dtype;
    tdesc->tileld   = (storage == matrix_Tile) ? mb : lm;
    tdesc->mb       = mb;
    tdesc->nb       = nb;
    tdesc->bsiz     = mb * nb;

    /* Large matrix parameters */
    tdesc->lm = lm;
    tdesc->ln = ln;

    /* Large matrix derived parameters */
    /* tdesc->lm1 = (lm/mb); */
    /* tdesc->ln1 = (ln/nb); */
    tdesc->lmt = (lm%mb==0) ? (lm/mb) : (lm/mb+1);
    tdesc->lnt = (ln%nb==0) ? (ln/nb) : (ln/nb+1);

    /* Update lm and ln to include the padding */
    if ( storage != matrix_Lapack ) {
        tdesc->lm = tdesc->lmt * tdesc->mb;
        tdesc->ln = tdesc->lnt * tdesc->nb;
    }

    /* Locally stored matrix dimensions */
    tdesc->llm = tdesc->lm;
    tdesc->lln = tdesc->ln;

    /* Submatrix parameters */
    tdesc->i = i;
    tdesc->j = j;
    tdesc->m = m;
    tdesc->n = n;

    /* Submatrix derived parameters */
    tdesc->mt = (i+m-1)/mb - i/mb + 1;
    tdesc->nt = (j+n-1)/nb - j/nb + 1;

    /* finish to update the main object properties */
#if defined(DAGUE_PROF_TRACE)
    o->key_to_string = tiled_matrix_key_to_string;
    asprintf(&(o->key_dim), "(%d, %d)", tdesc->lmt, tdesc->lnt);
#endif
}

void
tiled_matrix_desc_destroy( tiled_matrix_desc_t *tdesc )
{
    dague_matrix_destroy_data( tdesc );
    dague_ddesc_destroy( (dague_ddesc_t*)tdesc );
}


tiled_matrix_desc_t *
tiled_matrix_submatrix( tiled_matrix_desc_t *tdesc,
                        int i, int j, int m, int n)
{
    int mb, nb;
    tiled_matrix_desc_t *newdesc;

    mb = tdesc->mb;
    nb = tdesc->nb;

    if ( (i < 0) || ( (i%mb) != 0 ) ) {
        fprintf(stderr, "Invalid value of i\n");
        return NULL;
    }
    if ( (j < 0) || ( (j%nb) != 0 ) ) {
        fprintf(stderr, "Invalid value of j\n");
        return NULL;
    }
    if ( (m < 0) || ((m+i) > tdesc->lm) ) {
        fprintf(stderr, "Invalid value of m\n");
        return NULL;
    }
    if ( (n < 0) || ((n+j) > tdesc->ln) ) {
        fprintf(stderr, "Invalid value of n\n");
        return NULL;
    }

    if( tdesc->dtype & two_dim_block_cyclic_type ) {
        newdesc = (tiled_matrix_desc_t*) malloc ( sizeof(two_dim_block_cyclic_t) );
        memcpy( newdesc, tdesc, sizeof(two_dim_block_cyclic_t) );
    }
    else if( tdesc->dtype & sym_two_dim_block_cyclic_type ) {
        newdesc = (tiled_matrix_desc_t*) malloc ( sizeof(sym_two_dim_block_cyclic_t) );
        memcpy( newdesc, tdesc, sizeof(sym_two_dim_block_cyclic_t) );
    } else {
        fprintf(stderr, "Type not completely defined\n");
        return NULL;
    }

    // Submatrix parameters
    newdesc->i = i;
    newdesc->j = j;
    newdesc->m = m;
    newdesc->n = n;
    // Submatrix derived parameters
    newdesc->mt = (i+m-1)/mb - i/mb + 1;
    newdesc->nt = (j+n-1)/nb - j/nb + 1;
    return newdesc;
}

/* return a unique key (unique only for the specified dague_ddesc) associated to a data */
static uint32_t tiled_matrix_data_key(struct dague_ddesc_s *desc, ...)
{
    tiled_matrix_desc_t * Ddesc;
    unsigned int m, n;
    va_list ap;
    Ddesc = (tiled_matrix_desc_t*)desc;

    /* Get coordinates */
    va_start(ap, desc);
    m = va_arg(ap, unsigned int);
    n = va_arg(ap, unsigned int);
    va_end(ap);

    /* Offset by (i,j) to translate (m,n) in the global matrix */
    m += Ddesc->i / Ddesc->mb;
    n += Ddesc->j / Ddesc->nb;

    return ((n * Ddesc->lmt) + m);
}

#if defined(DAGUE_PROF_TRACE)
static int  tiled_matrix_key_to_string(struct dague_ddesc_s *desc, uint32_t datakey, char * buffer, uint32_t buffer_size)
/* return a string meaningful for profiling about data */
{
    tiled_matrix_desc_t * Ddesc;
    unsigned int m, n;
    int res;
    Ddesc = (tiled_matrix_desc_t*)desc;
    m = datakey % Ddesc->lmt;
    n = datakey / Ddesc->lmt;
    res = snprintf(buffer, buffer_size, "(%u, %u)", m, n);
    if (res < 0)
    {
        printf("error in key_to_string for tile (%u, %u) key: %u\n", m, n, datakey);
    }
    return res;
}
#endif /* DAGUE_PROF_TRACE */

/*
 * Writes the data into the file filename
 * Sequential function per node
 */
int tiled_matrix_data_write(tiled_matrix_desc_t *tdesc, char *filename)
{
    dague_ddesc_t *ddesc = &(tdesc->super);
    dague_data_t* data;
    FILE *tmpf;
    char *buf;
    int i, j, k;
    uint32_t myrank = tdesc->super.myrank;
    int eltsize =  dague_datadist_getsizeoftype( tdesc->mtype );

    tmpf = fopen(filename, "w");
    if(NULL == tmpf) {
        fprintf(stderr, "ERROR: The file %s cannot be open\n", filename);
        return -1;
    }

    if ( tdesc->storage == matrix_Tile ) {
        for (i = 0 ; i < tdesc->mt ; i++)
            for ( j = 0 ; j< tdesc->nt ; j++) {
                if ( ddesc->rank_of( ddesc, i, j ) == myrank ) {
                    data = ddesc->data_of( ddesc, i, j );
                    buf = dague_data_get_ptr(data, 0);
                    fwrite(buf, eltsize, tdesc->bsiz, tmpf );
                }
            }
    } else {
        for (i = 0 ; i < tdesc->mt ; i++)
            for ( j = 0 ; j< tdesc->nt ; j++) {
                if ( ddesc->rank_of( ddesc, i, j ) == myrank ) {
                    data = ddesc->data_of( ddesc, i, j );
                    buf = dague_data_get_ptr(data, 0);
                    for (k=0; k<tdesc->nb; k++) {
                        fwrite(buf, eltsize, tdesc->mb, tmpf );
                        buf += eltsize * tdesc->lm;
                    }
                }
            }
    }


    fclose(tmpf);
    return 0;
}

/*
 * Read the data from the file filename
 * Sequential function per node
 */
int tiled_matrix_data_read(tiled_matrix_desc_t *tdesc, char *filename)
{
    dague_ddesc_t *ddesc = &(tdesc->super);
    dague_data_t* data;
    FILE *tmpf;
    char *buf;
    int i, j, k, ret;
    uint32_t myrank = tdesc->super.myrank;
    int eltsize =  dague_datadist_getsizeoftype( tdesc->mtype );

    tmpf = fopen(filename, "w");
    if(NULL == tmpf) {
        fprintf(stderr, "ERROR: The file %s cannot be open\n", filename);
        return -1;
    }

    if ( tdesc->storage == matrix_Tile ) {
        for (i = 0 ; i < tdesc->mt ; i++)
            for ( j = 0 ; j< tdesc->nt ; j++) {
                if ( ddesc->rank_of( ddesc, i, j ) == myrank ) {
                    data = ddesc->data_of( ddesc, i, j );
                    buf = dague_data_get_ptr(data, 0);
                    ret = fread(buf, eltsize, tdesc->bsiz, tmpf );
                    if ( ret !=  tdesc->bsiz ) {
                        fprintf(stderr, "ERROR: The read on tile(%d, %d) read %d elements instead of %d\n",
                                i, j, ret, tdesc->bsiz);
                        return -1;
                    }
                }
            }
    } else {
        for (i = 0 ; i < tdesc->mt ; i++)
            for ( j = 0 ; j< tdesc->nt ; j++) {
                if ( ddesc->rank_of( ddesc, i, j ) == myrank ) {
                    data = ddesc->data_of( ddesc, i, j );
                    buf = dague_data_get_ptr(data, 0);
                    for (k=0; k < tdesc->nb; k++) {
                        ret = fread(buf, eltsize, tdesc->mb, tmpf );
                        if ( ret !=  tdesc->mb ) {
                            fprintf(stderr, "ERROR: The read on tile(%d, %d) read %d elements instead of %d\n",
                                    i, j, ret, tdesc->mb);
                            return -1;
                        }
                        buf += eltsize * tdesc->lm;
                    }
                }
            }
    }

    fclose(tmpf);
    return 0;
}
