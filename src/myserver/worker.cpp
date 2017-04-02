#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>
#include <pthread.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/work_queue.h"
#include "tools/cycle_timer.h"


#define NUM_THREADS 24

static struct Worker_state {
        pthread_t threads[NUM_THREADS];
        WorkQueue<Request_msg> normal_job_queue;
        WorkQueue<Request_msg> instant_job_queue;
} wstate;


// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
    std::ostringstream oss;
    oss << n;
    req.set_arg("cmd", "countprimes");
    req.set_arg("n", oss.str());
}


// Implements logic required by compareprimes command via multiple
// calls to execute_work.  This function fills in the appropriate
// response.
static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int params[4];
    int counts[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    for (int i = 0; i < 4; i++) {
        Request_msg dummy_req(0);
        Response_msg dummy_resp(0);
        create_computeprimes_req(dummy_req, params[i]);
        execute_work(dummy_req, dummy_resp);
        counts[i] = atoi(dummy_resp.get_response().c_str());
    }

    if (counts[1] - counts[0] > counts[3] - counts[2])
        resp.set_response("There are more primes in first range.");
    else
        resp.set_response("There are more primes in second range.");
}


void *normal_job_handler(void *threadarg) {
    while (1) {
        Request_msg req = wstate.normal_job_queue.get_work();
        Response_msg resp(req.get_tag());
        if (req.get_arg("cmd").compare("compareprimes") == 0) {
            execute_compareprimes(req, resp);
        } else {
            execute_work(req, resp);
        }

        worker_send_response(resp);
    }

    return NULL;
}

void *instant_job_handler(void *threadarg) {
    while (1) {
        Request_msg req = wstate.instant_job_queue.get_work();
        Response_msg resp(req.get_tag());
        execute_work(req, resp);
        worker_send_response(resp);
    }

    return NULL;
}


void worker_node_init(const Request_msg& params) {

    // This is your chance to initialize your worker.  For example, you
    // might initialize a few data structures, or maybe even spawn a few
    // pthreads here.  Remember, when running on Amazon servers, worker
    // processes will run on an instance with a dual-core CPU.

    DLOG(INFO) << "**** Initializing worker: "
            << params.get_arg("name")
            << " ****\n";

    wstate.instant_job_queue = WorkQueue<Request_msg>();
    wstate.normal_job_queue = WorkQueue<Request_msg>();
    for (int i = 1; i < NUM_THREADS; i++) {
        pthread_create(&wstate.threads[i],
                NULL,
                &normal_job_handler,
                NULL);
    }

    pthread_create(&wstate.threads[0], NULL, &instant_job_handler, NULL);
}


void worker_handle_request(const Request_msg& req) {
    DLOG(INFO) << "Worker got request: ["
                << req.get_tag()
                << ":"
                << req.get_request_string()
                << "]\n";

    if (req.get_arg("cmd") == "tellmenow")
        wstate.instant_job_queue.put_work(req);
    else
        wstate.normal_job_queue.put_work(req);
}
