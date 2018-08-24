#include <mpi.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "parsec/parsec_comm_engine.h"

#include "parsec/runtime.h"

int counter = 0;
int my_rank;

// Tag 0 for float
int
callback_tag_0(parsec_comm_engine_t *ce,
               parsec_ce_tag_t tag,
               void *msg,
               size_t msg_size,
               int src,
               void *cb_data)
{
    (void) ce; (void) cb_data;
    printf("[%d] In callback for tag %d, message sent from %d msg: %p size: %ld message: ", my_rank, tag, src, msg, msg_size);

    int i, total = msg_size/sizeof(int);

    int *buffer = (int *)msg;
    for(i = 0; i < total; i++) {
       printf("%d\t", buffer[i]);
    }
    printf("\n");
    counter++;

    return 1;
}

// Tag 1 for int
int
callback_tag_1(parsec_comm_engine_t *ce,
               parsec_ce_tag_t tag,
               void *msg,
               size_t msg_size,
               int src,
               void *cb_data)
{
    (void) ce; (void) cb_data;
    printf("[%d] In callback for tag %d, message sent from %d msg: %p size: %ld message: ", my_rank, tag, src, msg, msg_size);

    int i, total = msg_size/sizeof(float);

    float *buffer = (float *)msg;
    for(i = 0; i < total; i++) {
       printf("%f\t", buffer[i]);
    }
    printf("\n");
    counter++;

    return 1;
}

/* This structure is used to send handshake information for a PUT or GET */
typedef struct get_noti_am_s {
    parsec_ce_mem_reg_handle_t lreg;
    ptrdiff_t ldispl;
    parsec_ce_mem_reg_handle_t rreg;
    ptrdiff_t rdispl;
} get_noti_am_t;

/* This will be called in the receiver rank when GET is done */
int
get_end(parsec_comm_engine_t *ce,
        parsec_ce_mem_reg_handle_t lreg,
        ptrdiff_t ldispl,
        parsec_ce_mem_reg_handle_t rreg,
        ptrdiff_t rdispl,
        size_t size,
        int remote,
        void *cb_data)
{
    (void) ldispl; (void) rdispl; (void) size; (void) remote; (void) cb_data;
    void *mem;
    size_t mem_size;
    ce->mem_retrieve(lreg, &mem, &mem_size);

    printf("[%d] GET is over, message:\n", my_rank);
    int *test_buffer = (int *)mem;
    int i;
    for(i = 0; i < mem_size/(sizeof(int)); i++) {
        printf("%d\t", test_buffer[i]);
    }
    printf("\n");

    ce->mem_unregister(&lreg);

    free(test_buffer);

    get_noti_am_t noti_am;
    noti_am.lreg = rreg;
    noti_am.ldispl = 0;

    /* Send ACK to 0 to notify we are done with GET */
    ce->send_active_message(ce, 5, 0, &noti_am, sizeof(get_noti_am_t));

    counter++;

    return 1;
}

/* Active Message for GET notification */
int
notify_about_get(parsec_comm_engine_t *ce,
                 parsec_ce_tag_t tag,
                 void *msg,
                 size_t msg_size,
                 int src,
                 void *cb_data)
{
    (void) tag; (void) cb_data;
    assert(my_rank == 1);
    assert(msg_size == sizeof(get_noti_am_t));

    get_noti_am_t *noti_am = (get_noti_am_t *) msg;

    /* We have the remote mem_reg_handle.
     * Let's allocate the local mem_reg_handle
     * and let's start the GET.
     */
    parsec_ce_mem_reg_handle_t lreg;
    size_t lreg_size;
    int *test_buffer = malloc(sizeof(int) * 9);
    ce->mem_register(test_buffer, sizeof(int) * 9, NULL, &lreg, &lreg_size);

    /* Let's start the GET */
    ce->get(ce, lreg, 0, noti_am->lreg, noti_am->ldispl, 0, src, get_end, (void *) ce);

    counter++;

    return 1;
}

int
get_end_ack(parsec_comm_engine_t *ce,
            parsec_ce_tag_t tag,
            void *msg,
            size_t msg_size,
            int src,
            void *cb_data)
{
    (void) tag; (void) msg_size; (void) src; (void) cb_data;
    get_noti_am_t *noti_am = (get_noti_am_t *)msg;
    void *mem;
    size_t mem_size;
    ce->mem_retrieve(noti_am->lreg, &mem, &mem_size);

    printf("[%d] Notification of GET over received\n", my_rank);
    int *test_buffer = (int *)mem;

    free(test_buffer);

    ce->mem_unregister(&noti_am->lreg);

    counter++;

    return 1;
}

/* Active Message for PUT notification */
int
notify_about_put(parsec_comm_engine_t *ce,
                 parsec_ce_tag_t tag,
                 void *msg,
                 size_t msg_size,
                 int src,
                 void *cb_data)
{
    (void) tag; (void) src; (void) cb_data;
    assert(my_rank == 1);
    assert(msg_size == sizeof(get_noti_am_t));

    get_noti_am_t *noti_am = (get_noti_am_t *)msg;

    parsec_ce_mem_reg_handle_t lreg;
    size_t lreg_size;
    int *test_buffer = malloc(sizeof(int) * 9);
    ce->mem_register(test_buffer, sizeof(int) * 9, NULL, &lreg, &lreg_size);

    /* Let's do a put ack to pass the mem_reg of this side */
    get_noti_am_t noti_am_1;
    noti_am_1.lreg = lreg;
    noti_am_1.ldispl = 0;
    noti_am_1.rreg = noti_am->lreg;
    noti_am_1.rdispl = noti_am->ldispl;

    /* We have the remote mem_reg_handle.
     * Let's allocate the local mem_reg_handle
     * and send it to other side to start a PUT.
     */
    ce->send_active_message(ce, 7, 0, &noti_am_1, sizeof(get_noti_am_t));

    counter++;

    return 1;
}

/* This function will be called once the PUT is over */
int
put_end(parsec_comm_engine_t *ce,
        parsec_ce_mem_reg_handle_t lreg,
        ptrdiff_t ldispl,
        parsec_ce_mem_reg_handle_t rreg,
        ptrdiff_t rdispl,
        size_t size,
        int remote,
        void *cb_data)
{
    (void) ldispl; (void) rdispl; (void) size; (void) remote; (void) cb_data;
    printf("[%d] PUT is finished\n", my_rank);


    void *mem;
    size_t mem_size;
    ce->mem_retrieve(lreg, &mem, &mem_size);

    free(mem);

    ce->mem_unregister(&lreg);

    get_noti_am_t noti_am;
    noti_am.lreg = rreg;
    noti_am.ldispl = 0;

    /* Send ACK to 1 to notify we are done with PUT */
    ce->send_active_message(ce, 8, 1, &noti_am, sizeof(get_noti_am_t));

    counter++;

    return 1;
}

/* This function is called when the receiver has notified us to start the PUT */
int
put_ack_am(parsec_comm_engine_t *ce,
           parsec_ce_tag_t tag,
           void *msg,
           size_t msg_size,
           int src,
           void *cb_data)
{
    (void) tag; (void) cb_data;
    assert(my_rank == 0);
    assert(msg_size == sizeof(get_noti_am_t));

    get_noti_am_t *noti_am = (get_noti_am_t *)msg;

    printf("[%d] Received the remote mem_reg_handle and now can start the PUT\n", my_rank);

    /* We have received the mem_reg_handle of the other side, now we can
     * start the PUT */
    ce->put(ce, noti_am->rreg, noti_am->rdispl, noti_am->lreg, noti_am->ldispl, 0, src, put_end, NULL);

    counter++;

    return 1;
}

/* This function is called to notify the sender is done with PUT */
int
put_end_ack(parsec_comm_engine_t *ce,
            parsec_ce_tag_t tag,
            void *msg,
            size_t msg_size,
            int src,
            void *cb_data)
{
    (void) tag; (void) msg_size; (void) src; (void) cb_data;
    get_noti_am_t *noti_am = (get_noti_am_t *)msg;

    void *mem;
    size_t mem_size;
    ce->mem_retrieve(noti_am->lreg, &mem, &mem_size);

    printf("[%d] PUT is over, message:\n", my_rank);
    int *test_buffer = (int *)mem;
    int i;
    for(i = 0; i < mem_size/(sizeof(int)); i++) {
        printf("%d\t", test_buffer[i]);
    }
    printf("\n");

    free(test_buffer);

    ce->mem_unregister(&noti_am->lreg);

    counter++;

    return 1;
}


int main(int argc, char **argv)
{
    parsec_context_t *parsec = NULL;

    int rank, world, cores = 1;
    int i;

#if defined(PARSEC_HAVE_MPI)
    {
        int provided;
        MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &world);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#else
    world = 1;
    rank = 0;
#endif

    my_rank = rank;
    parsec = parsec_init( cores, &argc, &argv );

    parsec_comm_engine_t *ce = parsec_comm_engine_init(parsec);

    if( world != 2 ) {
        printf("World is too small, too bad! Buh-bye");
        return 0;
    }

    ce->tag_register(2, callback_tag_0, ce, 4096);
    ce->tag_register(3, callback_tag_1, ce, 4096);

    /* Active message for GET notification */
    ce->tag_register(4, notify_about_get, ce, 4096);
    ce->tag_register(5, get_end_ack, ce, 4096);

    /* Active message for PUT notification */
    ce->tag_register(6, notify_about_put, ce, 4096);
    ce->tag_register(7, put_ack_am, ce, 4096);
    ce->tag_register(8, put_end_ack, ce, 4096);


    /* To make sure all the ranks have the tags registered */
    MPI_Barrier(MPI_COMM_WORLD);

    /* Testing active message */
    if(rank == 0) {
        int   *intbuffer = NULL;
        intbuffer = malloc(3*sizeof(int));
        intbuffer[0] = 10;
        intbuffer[1] = 11;
        intbuffer[2] = 12;

        printf("[%d] Sending active message to 1, message: %d\t%d\t%d\n",
                my_rank, intbuffer[0], intbuffer[1], intbuffer[2]);

        ce->send_active_message(ce, 2, 1, intbuffer, 3*sizeof(int));
        ce->send_active_message(ce, 2, 1, intbuffer, 3*sizeof(int));

        while(counter != 2) {
            ce->progress(ce);
        }
        free(intbuffer);
    }

    if(rank == 1) {
        float *floatbuffer = NULL;
        floatbuffer = malloc(2*sizeof(float));
        floatbuffer[0] = 9.5;
        floatbuffer[1] = 19.5;

        printf("[%d] Sending active message to 0, message: %f\t%f\n",
                my_rank, floatbuffer[0], floatbuffer[1]);

        ce->send_active_message(ce, 3, 0, floatbuffer, 2*sizeof(float));
        ce->send_active_message(ce, 3, 0, floatbuffer, 2*sizeof(float));

        while(counter != 2) {
            ce->progress(ce);
        }
        free(floatbuffer);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    counter = 0;
    printf("-------------------------------------\n");

    /* Let's test Get from 1 -> 0 (1 gets from 0) */
    if(rank == 0) {
        parsec_ce_mem_reg_handle_t lreg;
        size_t lreg_size;
        int *test_buffer = malloc(sizeof(int) * 9);
        for(i = 0; i < 9; i++) {
            test_buffer[i] = i;
        }
        /* Registering a memory with a mem_reg_handle */
        ce->mem_register(test_buffer, sizeof(int) * 9, NULL, &lreg, &lreg_size);

        printf("[%d] Starting a GET (1 will get from 0), message:\n", my_rank);
        for(i = 0; i < 9; i++) {
            printf("%d\t", test_buffer[i]);
        }
        printf("\n");

        get_noti_am_t noti_am;
        noti_am.lreg   = lreg;
        noti_am.ldispl = 0;
        noti_am.rreg   = NULL;
        noti_am.rdispl = 0;

        /* 0 lets 1 know that it has some data for 1 to get */
        ce->send_active_message(ce, 4, 1, &noti_am, sizeof(get_noti_am_t));

        while(counter != 1) {
            ce->progress(ce);
        }
    }

    if(rank == 1) {
        while(counter != 2) {
            ce->progress(ce);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    counter = 0;

    printf("-------------------------------------\n");
    /* Let's test PUT from 0 -> 1 (0 puts in 1) */
    if(rank == 0) {
        parsec_ce_mem_reg_handle_t lreg;
        size_t lreg_size;
        int *test_buffer = malloc(sizeof(int) * 9);
        for(i = 0; i < 9; i++) {
            test_buffer[i] = i * 2;
        }
        ce->mem_register(test_buffer, sizeof(int) * 9, NULL, &lreg, &lreg_size);
        printf("[%d] Starting a PUT (0 will put in 1), message:\n", my_rank);
        for(i = 0; i < 9; i++) {
            printf("%d\t", test_buffer[i]);
        }
        printf("\n");


        get_noti_am_t noti_am;
        noti_am.lreg   = lreg;
        noti_am.ldispl = 0;
        noti_am.rreg   = NULL;
        noti_am.rdispl = 0;

        /* 0 lets 1 know that it has the data for 1 */
        ce->send_active_message(ce, 6, 1, &noti_am, sizeof(get_noti_am_t));

        while(counter != 2) {
            ce->progress(ce);
        }
    }

    if(rank == 1) {
        while(counter != 2)
            ce->progress(ce);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    ce->tag_unregister(2);
    ce->tag_unregister(3);
    ce->tag_unregister(4);
    ce->tag_unregister(5);
    ce->tag_unregister(6);
    ce->tag_unregister(7);
    ce->tag_unregister(8);

    parsec_comm_engine_fini(ce);

    parsec_fini(&parsec);

#ifdef PARSEC_HAVE_MPI
    MPI_Finalize();
#endif

    return 0;
}
