/*
 * Copyright (c) 2009      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */
#ifndef __SYM_TWO_DIM_RECTANGLE_CYCLIC_H__
#define __SYM_TWO_DIM_RECTANGLE_CYCLIC_H__

#include "parsec/data_dist/matrix/matrix.h"
#include "parsec/data_dist/matrix/grid_2Dcyclic.h"

BEGIN_C_DECLS

/*
 * Symmetrical matrix. 2D block cyclic distribution, lower tiles distributed only
 *
 * --
 *|0 |
 * --|--
 *|2 |3 |
 *|--|--|--
 *|0 |1 |0 |
 *|--|--|--|--
 *|2 |3 |2 |3 |
 * -----------
 *
 */

/*******************************************************************
 * distributed data structure and basic functionalities
 *******************************************************************/

/* structure equivalent to PLASMA_desc, but for distributed matrix data
 */

typedef struct sym_two_dim_block_cyclic {
    parsec_tiled_matrix_dc_t super;
    grid_2Dcyclic_t grid;
    void *mat;              /**< pointer to the beginning of the matrix */
    int uplo;
} sym_two_dim_block_cyclic_t;

/************************************************
 *   mpi ranks distribution in the process grid
 *   -----------------
 *   | 0 | 1 | 2 | 3 |
 *   |---------------|
 *   | 4 | 5 | 6 | 7 |
 *   -----------------
 ************************************************/


/**
 * Initialize the description of a  2-D block cyclic distributed matrix.
 * @param dc matrix description structure, already allocated, that will be initialize
 * @param nodes number of nodes
 * @param myrank rank of the local node (as of mpi rank)
 * @param mb number of row in a tile
 * @param nb number of column in a tile
 * @param lm number of rows of the entire matrix
 * @param ln number of column of the entire matrix
 * @param i starting row index for the computation on a submatrix
 * @param j starting column index for the computation on a submatrix
 * @param m number of rows of the entire submatrix
 * @param n numbr of column of the entire submatrix
 * @param kp number of rows of tiles for k-cyclic block distribution
 * @param kq number of column of tiles for k-cyclic block distribution
 * @param process_GridRows number of row of processes of the process grid (has to divide nodes)
 * @param uplo upper or lower triangular part of the matrix is kept
 */
void sym_two_dim_block_cyclic_init( sym_two_dim_block_cyclic_t * dc,
                                    enum matrix_type mtype,
                                    int nodes, int myrank,
                                    int mb, int nb, int lm, int ln,
                                    int i, int j, int m, int n,
                                    int kp, int kq,
                                    int process_GridRows, int uplo );

void sym_two_dim_block_cyclic_kview( sym_two_dim_block_cyclic_t* target,
                                     sym_two_dim_block_cyclic_t* origin,
                                     int kp, int kq );

static inline size_t sym_twoDBC_coordinates_to_position(sym_two_dim_block_cyclic_t *dc, int m, int n)
{
    size_t pos;
    int nb_elem, nb_elem_col, column;

    pos = 0; /* current position (as number of tile) in the buffer */
    column = dc->grid.crank; /* tile column considered */

    /**********************************/
    if(dc->uplo == matrix_Lower ) {
        nb_elem_col = (dc->super.lmt) / (dc->grid.rows); //nb of tile associated to that proc in a full column
        if( (dc->super.lmt) % (dc->grid.rows) > dc->grid.rrank )
            nb_elem_col++;

        while(column != n) {
            /* for each column of tiles in memory before searched element, compute the number of tile for displacement */
            nb_elem = column / (dc->grid.rows);
            if ( (column % (dc->grid.rows)) > dc->grid.rrank)
                nb_elem++;

            pos += (nb_elem_col - nb_elem);
            column += dc->grid.cols;
        }

        pos += ((m - n) / (dc->grid.rows));
    } else {
        while(column != n) {
            /* for each column of tiles in memory before searched element, compute the number of tile for displacement */
            nb_elem = (column + 1) / (dc->grid.rows);
            if ( ( (column + 1) % (dc->grid.rows)) > dc->grid.rrank)
                nb_elem++;

            pos += nb_elem;
            column += dc->grid.cols;
        }

        pos += (m / (dc->grid.rows));
    }
    return pos;
}

static inline size_t sym_twoDBC_kcyclic_coordinates_to_position(sym_two_dim_block_cyclic_t *dc, int m, int n)
{
    int pos = 0; /* current position (as number of tile) in the buffer */
    int disp_ktile_total, disp_tile_extra, disp_ktile_proc;

    /**********************************/
    if( dc->uplo == matrix_Lower ) {
        int row = (dc->grid.rrank) * (dc->grid.krows); /* tile row considered */
        /* for each row of tiles, compute number of tiles in that row */
        while( row < m ) {
            int krow = 0;
            /* iterate by k-row */
            while( (krow < dc->grid.krows) && ((row + krow) < m) ) {
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

                pos += nb_tile;
                krow++;
            }
            row += (dc->grid.rows) * (dc->grid.krows);
        }

        disp_ktile_total = n / (dc->grid.kcols);               /* nb of full k-tiles before n in row m */
        disp_tile_extra  = n % (dc->grid.kcols);               /* nb of extra tiles before n in row m */
        disp_ktile_proc  = disp_ktile_total / (dc->grid.cols); /* nb of k-tiles per process before n in row m */

        pos += (disp_ktile_proc * (dc->grid.kcols) + disp_tile_extra);
    } else { /* Upper */
        int col = (dc->grid.crank) * (dc->grid.kcols); /* tile column considered */
        /* for each column of tiles, compute number of tiles in that column */
        while( col < n ) {
            int kcol = 0;
            /* iterate by k-column */
            while( (kcol < dc->grid.kcols) && ((col + kcol) < n) ) {
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

                pos += nb_tile;
                kcol++;
            }
            col += (dc->grid.cols) * (dc->grid.kcols);
        }

        disp_ktile_total = m / (dc->grid.krows);               /* nb of full k-tiles before m in column n */
        disp_tile_extra  = m % (dc->grid.krows);               /* nb of extra tiles before m in column n */
        disp_ktile_proc  = disp_ktile_total / (dc->grid.rows); /* nb of k-tiles per process before m in column n */

        pos += (disp_ktile_proc * (dc->grid.krows) + disp_tile_extra);
    }

    return pos;
}

END_C_DECLS

#endif /* __TWO_DIM_RECTANGLE_CYCLIC_H__*/
