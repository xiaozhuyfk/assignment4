#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/work_queue.h"
#include "tools/cycle_timer.h"


#define NUM_THREADS 24

static struct Worker_state {
        cpu_set_t cpus[2];
        int thread_id[NUM_THREADS];
        pthread_t threads[NUM_THREADS];
        pthread_attr_t attribute[NUM_THREADS];
        WorkQueue<Request_msg> normal_job_queue[NUM_THREADS];
        //WorkQueue<Request_msg> instant_job_queue;
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
    int thread_id = *((int *) threadarg);
    while (1) {
        Request_msg req = wstate.normal_job_queue[thread_id].get_work();
        Response_msg resp(req.get_tag());
        if (req.get_arg("cmd").compare("compareprimes") == 0) {
            execute_compareprimes(req, resp);
        } else {
            execute_work(req, resp);
        }

        resp.set_thread_id(thread_id);
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

    int num_cores = 2; //sysconf(_SC_NPROCESSORS_ONLN);
    for (int core = 0; core < num_cores; core++) {
        CPU_ZERO(&wstate.cpus[core]);
        CPU_SET(core, &wstate.cpus[core]);

        int start = core * NUM_THREADS / 2;
        int end = start + NUM_THREADS / 2;
        for (int i = start; i < end; i++) {
            pthread_attr_init(&wstate.attribute[i]);

            pthread_attr_setaffinity_np(&wstate.attribute[i],
                    sizeof(cpu_set_t),
                    &wstate.cpus[core]);

            pthread_create(&wstate.threads[i],
                    &wstate.attribute[i],
                    &normal_job_handler,
                    (void *) &wstate.thread_id[i]);
        }

    }

}


void worker_handle_request(const Request_msg& req) {
    DLOG(INFO) << "Worker got request: ["
                << req.get_tag()
                << ":"
                << req.get_request_string()
                << "]\n";

    int thread_id = req.get_thread_id();
    wstate.normal_job_queue[thread_id].put_work(req);
}
