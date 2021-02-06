#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <stdbool.h>
#include <getopt.h>
#include <sys/eventfd.h> // linux only. if io_uring, eventfd is fine.
#include <poll.h>
#include <event2/event.h>
#include <event2/thread.h>
#include "mcmc.h"
#include "queue.h"
#include "pcg-basic.h"
#include "itoa_ljust.h"
#include "liburing.h"

// ... just use mcmc for testing?

#define BENCH_LIBEVENT 1
#define BENCH_URING 2
#define BENCH_LIBEVENT_BGSUBMIT 3
#define BENCH_LIBEVENT_TIMEOUTS 4

static int batch_size = 32;
static int conns_per_submit = 16;
static int threads_submit = 8;
static int threads_event = 1;
static int threads_backend = 1;
static int backend_conns = 150;
static int key_space = 100000;
static int total_backend_conns = 0;
bool use_eventfd = false; // FIXME: ugh.

typedef struct _submit_thread submit_thread;
typedef struct _conn conn;
typedef struct _event_thread event_thread;
typedef struct _backend backend;

typedef void (*proxy_event_cb)(void *udata, struct io_uring_cqe *cqe);

typedef struct {
    void *udata;
    proxy_event_cb cb;
} proxy_event_t;

typedef struct request {
    STAILQ_ENTRY(request) requests;
    mcmc_resp_t resp;
    char request_string[150];
    bool flushed;
    size_t request_len;
    conn *c;
    backend *be;
} request_t;
typedef STAILQ_HEAD(req_head_s, request) req_head_t;

#define READ_BUFFER_SIZE 16384

// mcmc client
struct _backend {
    bool connecting;
    bool stacked; // if stacked for a bg run already.
    struct event *read_event;
    struct event *write_event;
    struct event *timeout_event;
    proxy_event_t ur_read_event;
    proxy_event_t ur_write_event;
    char *buf; // dedicated read buffer per backend.
    void *client; // mcmc client.
    event_thread *evthr; // event thread owning this backend.
    STAILQ_ENTRY(_backend) be_next; // for work queue.
    pthread_mutex_t mutex;
    int depth; // run under mutex.
    req_head_t req_head;
};
// TODO: think we actually want a TAILQ for this one?
// _REMOVE() loops.
typedef STAILQ_HEAD(be_head_s, _backend) be_head_t;

// emulate a connection within a submission thread
struct _conn {
    struct request *req;
    unsigned int req_remaining;
    submit_thread *st;
};

struct _submit_thread {
    pthread_t thread_id;
    int notify_receive_fd;
    int notify_send_fd;
    bool event_submit;
    bool event_timer;
    pcg32_random_t rng;
    backend *backends;
    event_thread *event_threads;
    unsigned int notify_sent;
    pthread_mutex_t mutex;
};

typedef struct {
    pthread_t thread_id;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    event_thread *evthr;
} backend_thread;

struct _event_thread {
    pthread_t thread_id;
    int notify_receive_fd;
    int notify_send_fd;
    eventfd_t event_fd;
    struct io_uring ring;
    struct event_base *base;
    struct event *notify_event;
    struct timeval default_timeout;
    pthread_mutex_t req_mutex;
    pthread_cond_t cond;
    req_head_t req_head;
    be_head_t be_head; // stack of backends to broadcast.
    backend_thread *be_threads;
};


static void submit_requests(submit_thread *t, conn *c) {
    // BUG: need to write all requests at once...
    // else notify_receive_fd will get pinged too many times.
    // so we pre-load the req_remaining value before dispatching.

    pthread_mutex_lock(&t->mutex);
    c->req_remaining = batch_size;
    pthread_mutex_unlock(&t->mutex);

    for (int x = 0; x < batch_size; x++) {
        backend *be = &t->backends[pcg32_boundedrand_r(&t->rng, total_backend_conns)];
        pthread_mutex_lock(&be->mutex);

        // FIXME: build the rec outside of the be lock.
        struct request *r = &c->req[x];
        char *p = r->request_string;
        memcpy(r->request_string, "get toast", 9);
        p += 9;
        p = itoa_u32(pcg32_boundedrand_r(&t->rng, key_space), p);
        memcpy(p, "\r\n", 2); p += 2;
        r->request_len = p - r->request_string;
        //r->request_string[r->request_len] = 0;
        r->c = c;

        // add request to its queue
        STAILQ_INSERT_TAIL(&be->req_head, r, requests);
        be->depth++;

        // run the mcmc_submit() call inline.
        // TODO: check response for WANT_WRITE, err, etc.
        // TODO: need to safely check be->connecting and queue the writes.
        // for now lets just sidestep it by waiting a while.
        assert(be->connecting == false);
        mcmc_send_request(be->client, r->request_string, r->request_len, 1);
        //printf("WROTE [%d]\n", mcmc_fd(be->client));
        // TODO: exit() if get a WANT_WRITE for now.
        if (t->event_timer) {
            if (evtimer_pending(be->timeout_event, NULL) == 0) {
                struct timeval tmp_time = {5,0};
                // FIXME: for some reason using the default timeout
                // segfaults.
                //evtimer_add(be->timeout_event, &be->evthr->default_timeout);
                evtimer_add(be->timeout_event, &tmp_time);
            }
        }
        pthread_mutex_unlock(&be->mutex);
    }

}

// runs within event thread.
static void submit_requests_event(submit_thread *t, conn *c) {
    pthread_mutex_lock(&t->mutex);
    c->req_remaining = batch_size;
    pthread_mutex_unlock(&t->mutex);

    // FIXME: replace with array for multiple threads.
    req_head_t head;
    STAILQ_INIT(&head);

    for (int x = 0; x < batch_size; x++) {
        backend *be = &t->backends[pcg32_boundedrand_r(&t->rng, total_backend_conns)];

        struct request *r = &c->req[x];
        char *p = r->request_string;
        memcpy(r->request_string, "get toast", 9);
        p += 9;
        p = itoa_u32(pcg32_boundedrand_r(&t->rng, key_space), p);
        memcpy(p, "\r\n", 2); p += 2;
        r->request_len = p - r->request_string;
        //r->request_string[r->request_len] = 0;
        r->c = c;
        r->be = be;

        // add request to local queue
        STAILQ_INSERT_TAIL(&head, r, requests);
    }

    // - grab queue lock for event thread
    event_thread *e = &t->event_threads[0];
    pthread_mutex_lock(&e->req_mutex);
    // transplant our queue wholesale onto the event thread's
    // inbound queue. re-init's head for us.
    STAILQ_CONCAT(&e->req_head, &head);
    // - unlock and signal wake
    // Don't think there's any point in holding the lock since we're not doing
    // a condition wakeup.
    pthread_mutex_unlock(&e->req_mutex);
    if (use_eventfd) {
        uint64_t u = 1;
        int res = write(e->event_fd, &u, sizeof(uint64_t));
        assert(res == sizeof(uint64_t));
    } else {
        int res = write(e->notify_send_fd, "w", 1);
        assert(res > 0);
    }
}

static void *submit_thread_run(void *arg) {
    submit_thread *t = arg;
    conn *conns = calloc(conns_per_submit, sizeof(conn));
    for (int x = 0; x < conns_per_submit; x++) {
        conn *c = &conns[x];
        c->req = calloc(batch_size, sizeof(struct request));
    }
    pcg32_srandom_r(&t->rng, time(NULL), (intptr_t)conns);
    sleep(2); // hack to wait for backends to connect.

    // send initial batch of requests
    for (int i = 0; i < conns_per_submit; i++) {
        conn *c = &conns[i];
        c->st = t;
        if (t->event_submit) {
            submit_requests_event(t, c);
        } else {
            submit_requests(t, c);
        }
    }

    while (1) {
        // recv on the notify fd for our batched result.
        void *ptr;
        int bread = read(t->notify_receive_fd, &ptr, sizeof(ptr));
        conn *c = ptr;
        pthread_mutex_lock(&t->mutex);
        t->notify_sent--;
        if (c->req_remaining != 0) {
            printf("bread: %d doot: %lu rem: %d not: %d\n", bread, sizeof(ptr), c->req_remaining, t->notify_sent);
        }
        assert(bread == sizeof(ptr));
        assert(c->req_remaining == 0);
        pthread_mutex_unlock(&t->mutex);

        // connection's requests have been returned, now re-generate.
        if (t->event_submit) {
            submit_requests_event(t, c);
        } else {
            submit_requests(t, c);
        }
        //memset(reqs, 0, sizeof(struct request) * BATCH_SIZE);
    }

    //free(reqs);
    return NULL;
}

static void *event_thread_run(void *arg) {
    event_thread *t = arg;

    fprintf(stderr, "event thread starting\n");
    event_base_loop(t->base, 0);
    event_base_free(t->base);
    fprintf(stderr, "event thread exiting\n");
    return NULL;
}

// fan out from event thread.
static void *backend_thread_run(void *arg) {
    backend_thread *t = arg;
    pthread_mutex_lock(&t->mutex);
    while (1) {
        event_thread *ev = t->evthr;
        pthread_mutex_lock(&ev->req_mutex);
        if (STAILQ_EMPTY(&ev->be_head)) {
            pthread_cond_signal(&ev->cond);
            pthread_mutex_unlock(&ev->req_mutex);
            pthread_cond_wait(&t->cond, &t->mutex);
            continue;
        }

        // Got a backend to process.
        backend *be = STAILQ_FIRST(&ev->be_head);
        STAILQ_REMOVE_HEAD(&ev->be_head, be_next);
        pthread_mutex_unlock(&ev->req_mutex);

        // FIXME: backend mutex might not be necessary.
        pthread_mutex_lock(&be->mutex);
        be->stacked = false;

        assert(be->connecting == false);
        struct request *r;
        // FIXME: add another queue so we don't have to loop past stuff that
        // was written already?
        STAILQ_FOREACH(r, &be->req_head, requests) {
            if (r->flushed) {
                continue;
            }
            int status = mcmc_send_request(be->client, r->request_string, r->request_len, 1);
            if (status == MCMC_WANT_WRITE) {
                assert(1 == 0);
            }
            r->flushed = true;
        }

        pthread_mutex_unlock(&be->mutex);
    }
    return NULL;
}

static void backend_timeout_handler(int fd, short which, void *arg) {
    assert(1 == 0);
}

// NOTE:
// This externalizes the write syscalls from worker threads:
// - worker threads stack requests
// - pass to event thread
// - event thread runs batches of requests and executes fan-out
// - event thread then safely runs read events.
// for speedup, a step 1:
// - bg threads collect iovec's, run batched writes.
// io_uring, a step 2:
// - instead bg threads, iovecs are directly gathered into SQE's, executed,
// then write results checked inline before further reads.
static void backend_notify_handler(int fd, short which, void *arg) {
    event_thread *me = arg;
    char buf[1];
    // active work queue.
    req_head_t head;
    STAILQ_INIT(&head);
    STAILQ_INIT(&me->be_head);

    if (use_eventfd) {
        uint64_t u;
        if (read(fd, &u, sizeof(uint64_t)) != sizeof(uint64_t)) {
            fprintf(stderr, "failed to read from event notify fd\n");
            exit(1);
        }
    } else {
        if (read(fd, buf, 1) != 1) {
            fprintf(stderr, "failed to read from event notify fd\n");
            exit(1);
        }
    }

    // pull some work to chew on.
    pthread_mutex_lock(&me->req_mutex);
    STAILQ_CONCAT(&head, &me->req_head);
    pthread_mutex_unlock(&me->req_mutex);

    // TODO: with extra tracking, we can gather larger writes if a be has many
    // outstanding writes... but this will require tracking and re-looping all
    // backends?
    int be_count = 0;
    int req_count = 0;
    while(!STAILQ_EMPTY(&head)) {
        // - pull and lock backend.
        struct request *r = STAILQ_FIRST(&head);
        r->flushed = false;
        backend *be = r->be;
        // NOTES:
        // - if backend not queued, add to local queue
        // - roll all recs.
        //  - can assemble an iovec during this pass.
        // - at end of stack loop from backends for dispatch
        //  - keep counter for # of queued backends
        //  - wake BG threads up to depth
        //  - BG threads lock BE stack, pop, do work
        //   - move finished BE's to outbound queue?
        //   - think they can be dropped since we're just waiting for read
        //     events now?
        //   - actually no.. can only be dropped if work complete.
        //     re-queue for any WANT_WRITES/errors/etc.
        //  - if queue empty, signal event thread
        //  - issue: ev thread is idle at this time.
        // -------------
        // - could run tests maybe... wake per res BE vs full batch.
        //  - keeps thread busier, might have better latency?
        // - letting the reads run causes the same race condition tho.
        pthread_mutex_lock(&be->mutex);
        // TODO: handle WANT_WRITE / write failures.
        // - if be is writable, run write
        //  - if write fails, mark !writable
        //  - assign write event to event thread.
        // - either way, stack request to be tail
        // have to remove from local head before inserting to *be tail.
        STAILQ_REMOVE_HEAD(&head, requests);
        STAILQ_INSERT_TAIL(&be->req_head, r, requests);
        be->depth++;
        req_count++;
        if (!be->stacked) {
            be->stacked = true;
            STAILQ_INSERT_TAIL(&me->be_head, be, be_next);
            be_count++;
        }
        pthread_mutex_unlock(&be->mutex);
    }

    // requests are now stacked into per-backend queues.
    // we do this here to avoid worker threads blocking on backend mutexes
    // while flushing write syscalls.
    //fprintf(stderr, "signalling: %d [%d]\n", be_count, req_count);
    for (int x = 0; x < threads_backend; x++) {
        backend_thread *bt = &me->be_threads[x];
        pthread_cond_signal(&bt->cond);
        if (x == be_count)
            break;
    }

    // TODO: different mutex?
    pthread_mutex_lock(&me->req_mutex);
    if (!STAILQ_EMPTY(&me->be_head)) {
        pthread_cond_wait(&me->cond, &me->req_mutex);
    }
    pthread_mutex_unlock(&me->req_mutex);

    // TODO: re-lock and check inbound queue for WANT_WRITE/error/etc.
}

static void backend_read_handler(int fd, short which, void *arg) {
    backend *be = arg;
    char buf[256];

    pthread_mutex_lock(&be->mutex);
    if (STAILQ_EMPTY(&be->req_head)) {
        // normally this would close/reopen/something.
        int bread = read(mcmc_fd(be->client), buf, 256);
        buf[bread-1] = '\0';
        printf("woke up EV_READ with an empty queue: %s\n", buf);

        exit(1);
    }
    if (be->timeout_event && evtimer_pending(be->timeout_event, NULL) != 0) {
        evtimer_del(be->timeout_event);
    }

    // TODO: might be worth shortening the lock by grabbing first,
    // REMOVE_HEAD, then INSERT_HEAD under re-lock only if we need to.
    // most of the time it should execute directly anyway, avoiding holding
    // the lock while doing syscalls.
    int remain = 0;
    while (!STAILQ_EMPTY(&be->req_head)) {
        struct request *r = STAILQ_FIRST(&be->req_head);
        //printf("LOOPIN\n");
        int status = mcmc_read(be->client, buf, 256, &r->resp);
        if (status == MCMC_OK) {
            // all done, next.
            // FIXME: how do I know I got a partial read on a value?
        } else if (status == MCMC_WANT_READ) {
            printf("couldn't read full response\n");
            exit(1);
            // FIXME: offset reads will break.
        } else {
            printf("bad status: %d\n", status);
            exit(1);
        }

        pthread_mutex_lock(&r->c->st->mutex);

        STAILQ_REMOVE_HEAD(&be->req_head, requests);
        be->depth--;
        r->c->req_remaining--;
        if (r->c->req_remaining == 0) {
            // TODO: error/size check
            // TODO: note r->st is pointless but will make sense when it's a
            // conn ptr instead.
            r->c->st->notify_sent++; // FIXME: Debugging.
            int res = write(r->c->st->notify_send_fd, &r->c, sizeof(void *));
            if (res <= 0) {
                assert(1 == 0);
            }
        }
        pthread_mutex_unlock(&r->c->st->mutex);

        remain = 0;
        char *newbuf = mcmc_buffer_consume(be->client, &remain);
        if (remain != 0) {
            memmove(buf, newbuf, remain);
        } else {
            break;
        }
    }
    assert(remain == 0);

    pthread_mutex_unlock(&be->mutex);
}

static void backend_write_handler(int fd, short which, void *arg) {
    backend *be = arg;
    if (which & EV_TIMEOUT) {
        // died waiting for a write.
        printf("timed out writing\n");
        exit(1);
    }

    // only otherwise called on write.
    if (be->connecting) {
        // FIXME: does this mean be->connecting needs to be covered by mutex?
        // TODO: if we were connecting, flush queue of requests.
        be->connecting = false;
        return;
    }
}

#define SUBMIT_MODE_WORKER 0
#define SUBMIT_MODE_EVENT 1
#define SUBMIT_MODE_WORKER_TIMERS 2
static void setup_libevent_bench(int submit_mode) {
    // setup libevent
    // FIXME: this hates blocking pipes... but I want pipes to block on write?
    // hmm... only need nonblock on the read end?
    //event_enable_debug_mode();
    evthread_use_pthreads();
    backend *b = calloc(total_backend_conns, sizeof(backend));
    int be_cnt = 0;

    // thread[s] for libevent
    event_thread *e_threads = calloc(threads_event, sizeof(event_thread));
    // threads for backend write syscalls.
    backend_thread *b_threads = calloc(threads_backend, sizeof(backend_thread));
    // threads for submissions
    submit_thread *s_threads = calloc(threads_submit, sizeof(submit_thread));
    for (int i = 0; i < threads_submit; i++) {
        submit_thread *t = &s_threads[i];
        int fds[2];
        if (pipe(fds)) {
            perror("can't create proxy backend notify pipe");
            exit(1);
        }

        t->notify_receive_fd = fds[0];
        t->notify_send_fd = fds[1];
        t->backends = b;
        t->event_threads = e_threads;
        // use the event threads to write to backend sockets.
        if (submit_mode == SUBMIT_MODE_EVENT) {
            t->event_submit = true;
        } else if (submit_mode == SUBMIT_MODE_WORKER_TIMERS) {
            t->event_timer = true;
        }
        pthread_mutex_init(&t->mutex, NULL);

        pthread_create(&t->thread_id, NULL, submit_thread_run, t);
    }

    // init event threads.
    for (int i = 0; i < threads_event; i++) {
        event_thread *t = &e_threads[i];

        // FIXME: probably need a set of these _per_ event thread.
        // test will only work with 1 libevent thread for now.
        t->be_threads = b_threads;
        for (int x = 0; x < threads_backend; x++) {
            t->be_threads[x].evthr = t;
        }
        struct event_config *ev_config;
        ev_config = event_config_new();
        if (submit_mode == SUBMIT_MODE_WORKER_TIMERS) {
            // When working with timers and write-from-workers we'll need to
            // make edits from other threads.
            event_config_set_flag(ev_config, 0);
        } else {
            event_config_set_flag(ev_config, EVENT_BASE_FLAG_NOLOCK);
        }
        t->base = event_base_new_with_config(ev_config);
        event_config_free(ev_config);
        if (! t->base) {
            fprintf(stderr, "Can't allocate event base\n");
            exit(1);
        }

        int fds[2];
        if (pipe(fds)) {
            perror("can't create proxy backend notify pipe");
            exit(1);
        }

        t->notify_receive_fd = fds[0];
        t->notify_send_fd = fds[1];

        t->event_fd = eventfd(0, EFD_NONBLOCK);
        if (use_eventfd) {
            t->notify_event = event_new(t->base, t->event_fd, EV_READ | EV_PERSIST, backend_notify_handler, t);
        } else {
            t->notify_event = event_new(t->base, t->notify_receive_fd, EV_READ | EV_PERSIST, backend_notify_handler, t);
        }
        if (event_add(t->notify_event, 0) == -1) {
            fprintf(stderr, "Can't monitor libevent notify pipe\n");
            exit(1);
        }

        // FIXME: think this can just be zero?
        t->default_timeout.tv_sec = 5;
        t->default_timeout.tv_usec = 0;

        const struct timeval *tv_out = event_base_init_common_timeout(t->base, &t->default_timeout);
        memcpy(&t->default_timeout, tv_out, sizeof(struct timeval));

        for (int x = 0; x < backend_conns; x++) {
            backend *be = &b[be_cnt];
            be_cnt++;
            be->client = calloc(1, mcmc_size(MCMC_OPTION_BLANK));
            pthread_mutex_init(&be->mutex, NULL);
            STAILQ_INIT(&be->req_head);
            if (submit_mode == SUBMIT_MODE_WORKER_TIMERS) {
                be->timeout_event = evtimer_new(t->base, backend_timeout_handler, be);
            } else {
                be->timeout_event = NULL;
            }

            int status = mcmc_connect(be->client, "127.0.0.1", "11211", MCMC_OPTION_NONBLOCK);
            if (status == MCMC_CONNECTING) {
                // ok. need to wait until writable once.
                // TODO: event_add() the write event.
                // add be->connecting flag?
                be->connecting = true;
                be->read_event = event_new(t->base, mcmc_fd(be->client), EV_READ | EV_PERSIST,
                        backend_read_handler, be);
                be->write_event = event_new(t->base, mcmc_fd(be->client), EV_WRITE,
                        backend_write_handler, be);

                event_add(be->write_event, &t->default_timeout);
                event_add(be->read_event, 0);
                // NOTE: event_remove_timer();
            } else if (status == MCMC_CONNECTED) {
                printf("ERROR: connected\n");
                exit(1);
            } else {
                printf("can't connect\n");
                exit(1);
            }
        }

        pthread_mutex_init(&t->req_mutex, NULL);
        pthread_cond_init(&t->cond, NULL);
        STAILQ_INIT(&t->req_head);
        pthread_create(&t->thread_id, NULL, event_thread_run, t);
    }

    // threads for backend handling
    // NOTE: unused.
    for (int i = 0; i < threads_backend; i++) {
        backend_thread *t = &b_threads[i];
        pthread_mutex_init(&t->mutex, NULL);
        pthread_cond_init(&t->cond, NULL);

        pthread_create(&t->thread_id, NULL, backend_thread_run, t);
    }

}

////// IO_URING handlers

// TODO: need a compatible mcmc_read() that works from buffer.
static void backend_read_handler_ur(void *udata, struct io_uring_cqe *cqe) {
    backend *be = udata;
    int bread = cqe->res;

    pthread_mutex_lock(&be->mutex);
    if (STAILQ_EMPTY(&be->req_head)) {
        // normally this would close/reopen/something.
        //int bread = read(mcmc_fd(be->client), buf, 256);
        be->buf[bread-1] = '\0';
        printf("woke up READ[%d] with an empty queue: %s\n", bread, be->buf);

        exit(1);
    }

    //printf("bread: %d\n", bread);
    while (!STAILQ_EMPTY(&be->req_head)) {
        struct request *r = STAILQ_FIRST(&be->req_head);
        //printf("LOOPIN\n");
        int status = mcmc_parse_buf(be->client, be->buf, bread, &r->resp);
        if (status == MCMC_OK) {
            // all done, next.
            // FIXME: how do I know I got a partial read on a value?
        } else if (status == MCMC_WANT_READ) {
            printf("couldn't read full response\n");
            exit(1);
            // FIXME: offset reads will break.
        } else {
            printf("bad status: %d\n", status);
            exit(1);
        }

        pthread_mutex_lock(&r->c->st->mutex);

        STAILQ_REMOVE_HEAD(&be->req_head, requests);
        be->depth--;
        r->c->req_remaining--;
        if (r->c->req_remaining == 0) {
            // TODO: error/size check
            // TODO: note r->st is pointless but will make sense when it's a
            // conn ptr instead.
            r->c->st->notify_sent++; // FIXME: Debugging.
            int res = write(r->c->st->notify_send_fd, &r->c, sizeof(void *));
            if (res <= 0) {
                assert(1 == 0);
            }
        }
        pthread_mutex_unlock(&r->c->st->mutex);

        // FIXME: just push the buf ptr forward.
        size_t consumed = r->resp.reslen + r->resp.vlen_read;
        if (consumed == bread) {
            bread = 0;
            break;
        } else {
            bread -= consumed;
            memmove(be->buf, be->buf+consumed, bread);
        }
    }
    assert(bread == 0);

    // Need to re-submit the SQE every time.
    // FIXME: memmove any remaining buffer, and offset the next recv.
    pthread_mutex_unlock(&be->mutex);

    struct io_uring_sqe *sqe = io_uring_get_sqe(&be->evthr->ring);
    assert(sqe != NULL);

    io_uring_prep_recv(sqe, mcmc_fd(be->client), be->buf, READ_BUFFER_SIZE, 0);
    io_uring_sqe_set_data(sqe, &be->ur_read_event);

    // FIXME: need to submit in the uring_thread_run instead.
    io_uring_submit(&be->evthr->ring);
}

static void backend_write_handler_ur(void *udata, struct io_uring_cqe *cqe) {
    backend *be = udata;

    // TODO: validate connection / cqe's res code.
    if (be->connecting) {
        be->connecting = false;
        return;
    }
}

static void *uring_thread_run(void *arg) {
    event_thread *t = arg;
    struct io_uring_cqe *cqe;

    fprintf(stderr, "uring thread starting\n");
    while (1) {
        // FIXME: are we supposed to call this in a loop?
        // check liburing if it does pending cqe early exit.
        // NOTE: it does early exit, but there's a lot going on.
        // wait_cqes() can return an array.
        // can also try submit_and_wait() and cut submit from prep.
        int ret = io_uring_wait_cqe(&t->ring, &cqe);
        if (ret) {
            fprintf(stderr, "cqe:%d\n", ret);
            exit(1);
        }

        proxy_event_t *pe = io_uring_cqe_get_data(cqe);
        pe->cb(pe->udata, cqe);

        io_uring_cqe_seen(&t->ring, cqe);
    }
    return NULL;
}

#define QUEUE_ENTRIES 512

// TODO:
// eventfd_t's for submit thread(s) (conditional?)
// create one ring, then use WQ_ATTACH for rest (do we hold/leak the first
// ring?)
// submit: lock backend, attach req to queue, unlock backend, eventfd_write()
// backend: wake, lock queue, etc
//  - FIXME: queue on the backend thread? lock this and add there?
//  - request needs reference to backend object, which then gets locked
//  again/etc.
// event: I think ENTER_GETEVENTS will block?
//  - also test impact of adding the timeout and a loop.
//  - should be able to get away with a once-per-second wakeup.
// ... just malloc a receive buffer for each backend conn for now?
// later:
//  - IOSQE_BUFFER_SELECT
//  - check for FEAT_FAST_POLL (and B_S, etc?)
//    - FAST_POLL affects recv(), not read().
// FIXME: need to pin same backends to the submit thread.
static void setup_uring_bench(void) {
    // NOTE: just for my ... notes..
    /*struct __kernel_timespec ts;
    ts.tv_sec = 1;
    ts.tv_nsec = 0;*/

    struct io_uring ring;
    // dummy ring for workqueue reuse later.
    io_uring_queue_init(8, &ring, 0);

    struct io_uring_sqe *sqe;
    //struct io_uring_cqe *cqe;

    //sqe = io_uring_get_sqe(&ring);
    // if NULL, ring is full.
    // io_uring_prep_readv(sqe, fd, &iovec, 1, offset);

    //io_uring_submit(&ring);
    //io_uring_wait_cqe(&ring, &cqe);

    // do stuff with cqe

    //io_uring_cqe_seen(&ring, cqe);

    // FIXME: remove above. it's just example code.

    // reserve memory for backend connections.
    backend *b = calloc(total_backend_conns, sizeof(backend));
    int be_cnt = 0;

    // threads for submissions (mc worker analogues)
    submit_thread *s_threads = calloc(threads_submit, sizeof(submit_thread));
    for (int i = 0; i < threads_submit; i++) {
        submit_thread *t = &s_threads[i];
        int fds[2];
        if (pipe(fds)) {
            perror("can't create proxy backend notify pipe");
            exit(1);
        }

        t->notify_receive_fd = fds[0];
        t->notify_send_fd = fds[1];
        t->backends = b;
        //t->uring_submit = true;
        pthread_mutex_init(&t->mutex, NULL);

        pthread_create(&t->thread_id, NULL, submit_thread_run, t);
    }

    // thread[s] for libevent
    event_thread *e_threads = calloc(threads_event, sizeof(event_thread));
    for (int i = 0; i < threads_event; i++) {
        event_thread *t = &e_threads[i];
        struct io_uring_params p;
        memset(&p, 0, sizeof(p));

        p.flags = IORING_SETUP_ATTACH_WQ; // reuse thread pool.
        p.wq_fd = ring.ring_fd; // fd from our dummy ring.
        int ret = io_uring_queue_init_params(QUEUE_ENTRIES, &t->ring, &p);
        if (ret) {
            fprintf(stderr, "event uring failed\n");
            exit(1);
        }

        // FIXME: need our own timer system.
        t->default_timeout.tv_sec = 5;
        t->default_timeout.tv_usec = 0;

        for (int x = 0; x < backend_conns; x++) {
            backend *be = &b[be_cnt];
            be_cnt++;
            be->client = calloc(1, mcmc_size(MCMC_OPTION_BLANK));
            be->evthr = t;
            pthread_mutex_init(&be->mutex, NULL);
            STAILQ_INIT(&be->req_head);

            int status = mcmc_connect(be->client, "127.0.0.1", "11211", MCMC_OPTION_NONBLOCK);
            if (status == MCMC_CONNECTING) {
                // ok. need to wait until writable once.
                be->connecting = true;
                sqe = io_uring_get_sqe(&t->ring);
                if (!sqe) {
                    fprintf(stderr, "no sqe for backend connect\n");
                    exit(1);
                }

                // FIXME: also for HUP/ERR.
                // FIXME: is this actually backed by poll()? do we need to set
                // up epoll for this kind of thing?
                be->ur_write_event.cb = backend_write_handler_ur;
                be->ur_write_event.udata = be;

                io_uring_prep_poll_add(sqe, mcmc_fd(be->client), POLLOUT);
                io_uring_sqe_set_data(sqe, &be->ur_write_event);
                // TODO: do we set the read event here or post-connect?
                // TODO: no timeout.
                sqe = io_uring_get_sqe(&t->ring);
                if (!sqe) {
                    fprintf(stderr, "no sqe for backend read\n");
                    exit(1);
                }

                be->ur_read_event.cb = backend_read_handler_ur;
                be->ur_read_event.udata = be;
                be->buf = malloc(READ_BUFFER_SIZE);
                // under FAST_POLL, specifically nonblocking RECV will wait
                // until data is available before returning.
                io_uring_prep_recv(sqe, mcmc_fd(be->client), be->buf, READ_BUFFER_SIZE, 0);
                io_uring_sqe_set_data(sqe, &be->ur_read_event);

            } else if (status == MCMC_CONNECTED) {
                printf("ERROR: connected\n");
                exit(1);
            } else {
                printf("can't connect\n");
                exit(1);
            }
        }
        io_uring_submit(&t->ring);

        pthread_create(&t->thread_id, NULL, uring_thread_run, t);
    }

    // threads for backend handling
    // FIXME: probably unused for uring.
    backend_thread *b_threads = calloc(threads_backend, sizeof(backend_thread));
    for (int i = 0; i < threads_backend; i++) {
        backend_thread *t = &b_threads[i];
        pthread_mutex_init(&t->mutex, NULL);
        pthread_cond_init(&t->cond, NULL);

        pthread_create(&t->thread_id, NULL, backend_thread_run, t);
    }

}

int main (int argc, char *argv[]) {
    int type = BENCH_LIBEVENT;
    int opt;
    char *shortopts =
        "b:" // request batch size
        "c:" // fake client conns per submit thread
        "s:" // submit/worker threads
        "e:" // event threads
        "k:" // key space.
        "n:" // backend connections per event thread
        "t:" // type of test to run
        "f"  // use eventfd instead of notify pipe.
        "q:" // number of submit threads.
        ;
    while ((opt = getopt(argc, argv, shortopts)) != -1) {
        switch (opt) {
            case 'b':
                batch_size = atoi(optarg);
                break;
            case 'c':
                conns_per_submit = atoi(optarg);
                break;
            case 's':
                threads_submit = atoi(optarg);
                break;
            case 'e':
                threads_event = atoi(optarg);
                break;
            case 'k':
                key_space = atoi(optarg);
                break;
            case 'n':
                backend_conns = atoi(optarg);
                break;
            case 'q':
                threads_backend = atoi(optarg);
                break;
            case 't':
                if (optarg[0] == 'e') {
                    type = BENCH_LIBEVENT;
                } else if (optarg[0] == 'u') {
                    type = BENCH_URING;
                } else if (optarg[0] == 'b') {
                    type = BENCH_LIBEVENT_BGSUBMIT;
                } else if (optarg[0] == 't') {
                    type = BENCH_LIBEVENT_TIMEOUTS;
                } else {
                    exit(1);
                }
                break;
            case 'f':
                use_eventfd = true;
                break;
            default:
                fprintf(stderr, "bad argument\n");
                exit(1);
        }
    }
    total_backend_conns = backend_conns * threads_event;

    if (type == BENCH_LIBEVENT) {
        setup_libevent_bench(0);
    } else if (type == BENCH_LIBEVENT_BGSUBMIT) {
        setup_libevent_bench(1);
    } else if (type == BENCH_LIBEVENT_TIMEOUTS) {
        setup_libevent_bench(2);
    } else if (type == BENCH_URING) {
        setup_uring_bench();
    } else {
        exit(1);
    }

    sleep(999);
    return 0;
}
