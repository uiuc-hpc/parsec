/* parsec things */
#include "parsec/runtime.h"

/* system and io */
#include <stdlib.h>
#include <stdio.h>

#include "common_timing.h"
#include "parsec/interfaces/superscalar/insert_function_internal.h"
#include "parsec/utils/debug.h"

#if defined(PARSEC_HAVE_STRING_H)
#include <string.h>
#endif  /* defined(PARSEC_HAVE_STRING_H) */

#if defined(PARSEC_HAVE_MPI)
#include <mpi.h>
#elif defined(PARSEC_HAVE_LCI)
#include <lc.h>
#endif  /* defined(PARSEC_HAVE_MPI) */

double time_elapsed = 0.0;
double sync_time_elapsed = 0.0;

int32_t count = 0;

int
test_task( parsec_execution_stream_t *es,
           parsec_task_t *this_task )
{
    (void)es;

    int amount_of_work;
    parsec_dtd_unpack_args(this_task, &amount_of_work);

    (void)parsec_atomic_fetch_inc_int32(&count);

    int i, j;
    for( i = 0; i < amount_of_work; i++ ) {
        for( j = 0; j < amount_of_work/2; j++ ) {
            i = j*2;
            j = j + 20;
            i = j*2;
        }
    }

    return PARSEC_HOOK_RETURN_DONE;
}

int
test_task_generator( parsec_execution_stream_t *es,
                     parsec_task_t *this_task )
{
    (void)es;

    parsec_taskpool_t *dtd_tp = this_task->taskpool;
    int total, step, *iteration, amount_of_work;
    int i;

    parsec_dtd_unpack_args(this_task, &amount_of_work, &total, &step,
                           &iteration);

    for( i = 0; *iteration < total; *iteration += 1, i++ ) {
        if( i > step ) {
            return PARSEC_HOOK_RETURN_AGAIN;
        } else {
            parsec_dtd_taskpool_insert_task(dtd_tp, test_task,    0,  "Test_Task",
                                            sizeof(int),      &amount_of_work,    VALUE,
                                            PARSEC_DTD_ARG_END);

        }
    }

    return PARSEC_HOOK_RETURN_DONE;
}

int main(int argc, char ** argv)
{
    parsec_context_t* parsec;
    int rank, world, cores = -1, rc;

    if(argv[1] != NULL){
        cores = atoi(argv[1]);
    }

#if defined(PARSEC_HAVE_MPI)
    {
        int provided;
        MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &provided);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &world);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#elif defined(PARSEC_HAVE_LCI)
    lc_ep ep;
    lc_init(1, &ep);
    lci_global_ep = &ep;
    lc_get_proc_num(&rank);
    lc_get_num_proc(&world);
#else
    world = 1;
    rank = 0;
#endif

    int m, n;
    int no_of_tasks = 500000;
    int amount_of_work[3] = {100, 1000, 10000};
    parsec_taskpool_t *dtd_tp;

    parsec = parsec_init( cores, &argc, &argv );

    dtd_tp = parsec_dtd_taskpool_new();

    /* Registering the dtd_handle with PARSEC context */
    rc = parsec_context_add_taskpool( parsec, dtd_tp );
    PARSEC_CHECK_ERROR(rc, "parsec_context_add_taskpool");
    rc = parsec_context_start( parsec );
    PARSEC_CHECK_ERROR(rc, "parsec_context_start");

    if( rank == 0 ) {
        parsec_output( 0, "In all the tests we insert tasks "
                       "that does varying amount of work. The operation is constant, we vary "
                       "the number of times we do the operation\n\n" );
    }

    int tmp_window_size, tmp_threshold_size;
    tmp_window_size    = parsec_dtd_window_size;
    tmp_threshold_size = parsec_dtd_threshold_size;

    parsec_dtd_window_size    = no_of_tasks;
    parsec_dtd_threshold_size = no_of_tasks;


    /****** Inserting tasks using main thread while others execute ******/
    if( rank == 0 ) {
        parsec_output( 0, "\nWe now insert %d tasks using the main thread while the others %d cores "
                       "executes them simultaneously, main thread joins after all tasks are inserted "
                       "\n\n", no_of_tasks, cores-1 );
    }

    for( n = 0; n < 3; n++ ) {
        count = 0;

        TIME_START();

        for( m = 0; m < no_of_tasks; m++ ) {
            parsec_dtd_taskpool_insert_task(dtd_tp, test_task,    0,  "Test_Task",
                                            sizeof(int),      &amount_of_work[n], VALUE,
                                            PARSEC_DTD_ARG_END);
        }

        /* finishing all the tasks inserted, but not finishing the handle */
        rc = parsec_dtd_taskpool_wait( dtd_tp );
        PARSEC_CHECK_ERROR(rc, "parsec_dtd_taskpool_wait");

        TIME_PRINT(rank, ("Tasks executed : %d : Amount of work: %d\n", count, amount_of_work[n]));
    }
    /****** END ******/

    count = 0;
    parsec_dtd_window_size    = tmp_window_size;
    parsec_dtd_threshold_size = tmp_threshold_size;

    /****** Inserting tasks using main thread while others execute ******/
    if( rank == 0 ) {
        parsec_output( 0, "\nWe now insert %d tasks using the main thread while the other %d cores "
                       "executes them simultaneously, the main thread also joins the others to "
                       "execute following a sliding window\n\n", no_of_tasks, cores-1 );
    }

    for( n = 0; n < 3; n++ ) {
        count = 0;

        TIME_START();

        for( m = 0; m < no_of_tasks; m++ ) {
            parsec_dtd_taskpool_insert_task(dtd_tp, test_task,    0,  "Test_Task",
                                            sizeof(int),      &amount_of_work[n], VALUE,
                                            PARSEC_DTD_ARG_END);
        }

        /* finishing all the tasks inserted, but not finishing the handle */
        rc = parsec_dtd_taskpool_wait( dtd_tp );
        PARSEC_CHECK_ERROR(rc, "parsec_dtd_taskpool_wait");
        TIME_PRINT(rank, ("Tasks executed : %d : Amount of work: %d\n", count, amount_of_work[n]));
    }
    /****** END ******/

    /****** All threads insert and all threads execute ******/
    if( rank == 0 ) {
        parsec_output( 0, "\nWe now insert %d tasks using all threads and is also executed "
                       "by the all of them\n\n", no_of_tasks, cores-1 );
    }

    for( n = 0; n < 3; n++ ) {
        count = 0;

        TIME_START();

        int step = parsec_dtd_window_size, iteration = 0;
        parsec_dtd_taskpool_insert_task(dtd_tp, test_task_generator,    0,  "Test_Task",
                                        sizeof(int),      &amount_of_work[n],     VALUE,
                                        sizeof(int),      &no_of_tasks,           VALUE,
                                        sizeof(int),      &step,                  VALUE,
                                        sizeof(int),      &iteration,             REF,
                                        PARSEC_DTD_ARG_END);

        /* finishing all the tasks inserted, but not finishing the handle */
        rc = parsec_dtd_taskpool_wait( dtd_tp );
        PARSEC_CHECK_ERROR(rc, "parsec_dtd_taskpool_wait");

        TIME_PRINT(rank, ("Tasks executed : %d : Amount of work: %d\n", count, amount_of_work[n]));

    }
    /****** END ******/

    rc = parsec_context_wait(parsec);
    PARSEC_CHECK_ERROR(rc, "parsec_context_wait");

    parsec_taskpool_free( dtd_tp );

    parsec_fini(&parsec);

#ifdef PARSEC_HAVE_MPI
    MPI_Finalize();
#elif defined(PARSEC_HAVE_LCI)
    lc_finalize();
#endif

    return 0;
}
