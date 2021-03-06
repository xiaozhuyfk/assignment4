#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <assert.h>
#include <string>

#include <map>
#include <vector>
#include <queue>
#include <set>

#include "server/messages.h"
#include "server/master.h"
#include "tools/work_queue.h"


#define NUM_THREADS 24
#define JOB_COUNT_THREASHOLD 48
#define WORK_THREASHOLD 200


/*
 * Helper function headers
 */

int get_thread_id(Request_msg& req) {
    return req.get_tag() % 100;
}

void set_thread_id(Request_msg& req, int id) {
    req.set_tag((req.get_tag() / 100) * 100 + id);
}

int work_estimate(Request_msg& req);

Worker_handle find_best_receiver(Request_msg& req);

void request_new_worker();

void compute_cmp_prime_resp(
        Response_msg& cmp_prime_resp,
        std::vector<Response_msg> prime_resp);

void distribute_job(Request_msg& req);

void distribute_job_to_worker(Worker_handle worker, Request_msg& req);

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
    std::ostringstream oss;
    oss << n;
    req.set_arg("cmd", "countprimes");
    req.set_arg("n", oss.str());
}





/*
 * Master server routine
 */

struct Worker_state {
        int job_count;
        int instant_job_count;
        int idle_time;
        bool processing_cached_job[2];
        std::vector<int> work_estimate;
};


static struct Master_state {

        // The mstate struct collects all the master node state into one
        // place.  You do not need to preserve any of the fields below, they
        // exist only to implement the basic functionality of the starter
        // code.

        bool server_ready;
        int max_num_workers;
        int requested_workers;
        int next_tag;
        Worker_handle first_worker;

        // all workers alive
        std::map<Worker_handle, Worker_state> worker_roster;

        // tag-client map
        std::map<int, Client_handle> client_mapping;

        // tag-request map
        std::map<int, Request_msg> request_mapping;

        // queue of requests that not assigned to workers
        std::queue<int> pending_requests;

        // queue of cached jobs that not assigned to workers
        std::queue<int> pending_cached_jobs;

        // cache map
        std::map<std::string, Response_msg> cache_map;

        // compare_prime map from tag to Request_msg
        std::map<int, std::vector<Response_msg>> cmp_prime_map;

        // find the head tag for compare_prime request
        std::map<int, int> tag_head_map;

} mstate;

void master_node_init(int max_workers, int& tick_period) {

    tick_period = 1;

    mstate.next_tag = 0;
    mstate.max_num_workers = max_workers;
    mstate.requested_workers = 0;
    mstate.first_worker = NULL;

    // don't mark the server as ready until the server is ready to go.
    // This is actually when the first worker is up and running, not
    // when 'master_node_init' returnes
    mstate.server_ready = false;

    // fire off a request for a new worker
    request_new_worker();
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

    // 'tag' allows you to identify which worker request this response
    // corresponds to.  Since the starter code only sends off one new
    // worker request, we don't use it here.

    mstate.requested_workers--;
    mstate.worker_roster[worker_handle].job_count = 0;
    mstate.worker_roster[worker_handle].instant_job_count = 0;
    mstate.worker_roster[worker_handle].idle_time = 0;
    mstate.worker_roster[worker_handle].processing_cached_job[0] = false;
    mstate.worker_roster[worker_handle].processing_cached_job[1] = false;
    mstate.worker_roster[worker_handle].work_estimate =
            std::vector<int>(NUM_THREADS, 0);

    if (mstate.first_worker == NULL) mstate.first_worker = worker_handle;

    // Now that a worker is booted, let the system know the server is
    // ready to begin handling client requests.  The test harness will
    // now start its timers and start hitting your server with requests.
    if (mstate.server_ready == false) {
        server_init_complete();
        mstate.server_ready = true;
    }
}


void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

    // Master node has received a response from one of its workers.
    // Here we directly return this response to the client.

    DLOG(INFO) << "Master received a response from a worker: ["
            << resp.get_tag()
            << ":"
            << resp.get_response()
            << "]"
            << std::endl;

    int tag = resp.get_tag() / 100;
    int thread_id = resp.get_tag() % 100;
    Request_msg req = mstate.request_mapping[tag];
    Client_handle client = mstate.client_mapping[tag];
    Worker_state& wstate = mstate.worker_roster[worker_handle];
    std::string job = req.get_arg("cmd");

    // decrement job count
    if (job != "tellmenow") wstate.job_count--;
    else wstate.instant_job_count--;

    // restore the work estimate entry for the worker
    wstate.work_estimate[thread_id] -= work_estimate(req);
    assert(wstate.work_estimate[thread_id] >= 0);

    // Deal with compare prime response
    if (mstate.tag_head_map.find(tag) != mstate.tag_head_map.end()) {
        int tag_head = mstate.tag_head_map[tag];
        mstate.cmp_prime_map[tag_head][tag - tag_head - 1] = resp;
        bool prime_all_finish = true;

        for (Response_msg& r : mstate.cmp_prime_map[tag_head]) {
            if (r.get_tag() == -1) {
                prime_all_finish = false;
                break;
            }
        }

        if (prime_all_finish) {
            Response_msg cmp_prime_resp(tag_head);
            compute_cmp_prime_resp(cmp_prime_resp,
                    mstate.cmp_prime_map[tag_head]);
            send_client_response(client, cmp_prime_resp);
        }
    } else {
        send_client_response(client, resp);
    }

    // erase the request from client/request mapping
    mstate.client_mapping.erase(tag);
    mstate.request_mapping.erase(tag);

    if (job == "projectidea") {
        assert(thread_id == 1 || thread_id == 2);
        wstate.processing_cached_job[thread_id - 1] = false;
    }

    if ((thread_id == 1 || thread_id == 2) &&
            mstate.pending_cached_jobs.size() > 0 ) {
        int tag = mstate.pending_cached_jobs.front();
        mstate.pending_cached_jobs.pop();
        Request_msg& req = mstate.request_mapping[tag];
        set_thread_id(req, thread_id);
        distribute_job_to_worker(worker_handle, req);
    } else {
        if (mstate.pending_requests.size() > 0 &&
                (worker_handle != mstate.first_worker || thread_id != 0)) {
            int tag = mstate.pending_requests.front();
            mstate.pending_requests.pop();
            Request_msg& req = mstate.request_mapping[tag];
            set_thread_id(req, thread_id);
            distribute_job_to_worker(worker_handle, req);
        }
    }

    // add response to cache
    if (job == "countprimes") {
        std::string k= req.get_arg("n");
        mstate.cache_map[k] = resp;
    }
}


void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

    DLOG(INFO) << "Received request: "
            << client_req.get_request_string()
            << std::endl;

    // You can assume that traces end with this special message.  It
    // exists because it might be useful for debugging to dump
    // information about the entire run here: statistics, etc.
    if (client_req.get_arg("cmd") == "lastrequest") {
        Response_msg resp(0);
        resp.set_response("ack");
        send_client_response(client_handle, resp);
        return;
    }

    // Fire off the request to the worker.  Eventually the worker will
    // respond, and your 'handle_worker_response' event handler will be
    // called to forward the worker's response back to the server.
    int tag = mstate.next_tag;
    Request_msg worker_req(tag * 100, client_req);
    mstate.client_mapping[tag] = client_handle;
    mstate.request_mapping[tag] = worker_req;
    mstate.next_tag =
            (client_req.get_arg("cmd") == "compareprimes") ?
            mstate.next_tag + 5 :
            mstate.next_tag + 1;


    // if we get a compare primes job
    if (worker_req.get_arg("cmd") == "compareprimes") {
        int params[4];
        int cmp_tag = tag + 1;

        // grab the four arguments defining the two ranges
        params[0] = atoi(worker_req.get_arg("n1").c_str());
        params[1] = atoi(worker_req.get_arg("n2").c_str());
        params[2] = atoi(worker_req.get_arg("n3").c_str());
        params[3] = atoi(worker_req.get_arg("n4").c_str());

        std::vector<Response_msg> r_msg(4, -1);
        mstate.cmp_prime_map[tag] = r_msg;

        bool all_cached_flag = true;
        for (int i = 0; i < 4; i++) {
            Request_msg dummy_req(cmp_tag * 100);

            create_computeprimes_req(dummy_req, params[i]);
            mstate.client_mapping[cmp_tag] = client_handle;
            mstate.request_mapping[cmp_tag] = dummy_req;


            std::string cmp_test_key = std::to_string(params[i]);

            // Record the original tag for compare_prime
            mstate.tag_head_map[cmp_tag] = tag;

            // Check if the request is cached
            if (mstate.cache_map.find(cmp_test_key) != mstate.cache_map.end()) {
                Response_msg resp = mstate.cache_map[cmp_test_key];
                mstate.cmp_prime_map[tag][i] = resp;
            } else {
                all_cached_flag = false;
                distribute_job(dummy_req);
            }
            cmp_tag++;
        }

        // if all count
        if (all_cached_flag) {
            Response_msg cmp_prime_resp(tag);
            compute_cmp_prime_resp(cmp_prime_resp, mstate.cmp_prime_map[tag]);
            send_client_response(client_handle, cmp_prime_resp);
        }
    // if it is not a compare primes job
    } else {
        if (worker_req.get_arg("cmd")=="countprimes") {
            std::string test_key = worker_req.get_arg("n");
            // Check if the request is cached
            if (mstate.cache_map.find(test_key) != mstate.cache_map.end()) {
                Response_msg resp = mstate.cache_map[test_key];
                send_client_response(client_handle, resp);
            // if it is an instant job, send to worker directly
            } else {
                distribute_job(worker_req);
            }
        } else {
            distribute_job(worker_req);
        }

        // scale up
        if (mstate.worker_roster.size() + mstate.requested_workers + 1 <
            mstate.max_num_workers) {
            if (mstate.pending_requests.size() > 24 ||
                    mstate.pending_cached_jobs.size() > 1) {
                request_new_worker();
                request_new_worker();
            } else if (mstate.pending_requests.size() > 16 ||
                    mstate.pending_cached_jobs.size() > 0) {
                request_new_worker();
            }
        } else if (mstate.worker_roster.size() + mstate.requested_workers <
                mstate.max_num_workers) {
            if (mstate.pending_requests.size() > 16 ||
                    mstate.pending_cached_jobs.size() > 0) {
                request_new_worker();
            }
        }
    }

    // We're done!  This event handler now returns, and the master
    // process calls another one of your handlers when action is
    // required.

}

void handle_tick() {

    for (auto &pair : mstate.worker_roster) {
        Worker_state& wstate = pair.second;
        if (wstate.instant_job_count == 0 && wstate.job_count == 0) {
            wstate.idle_time++;
        }
    }

    // discard idle workers
    for (auto &pair : mstate.worker_roster) {
        if (pair.first == mstate.first_worker) continue;

        Worker_state& wstate = pair.second;
        if (wstate.instant_job_count == 0 &&
                wstate.job_count == 0 &&
                wstate.idle_time > 1 &&
                mstate.worker_roster.size() > 1) {
            mstate.worker_roster.erase(pair.first);
            kill_worker_node(pair.first);
            break;
        }
    }
}



/*
 * Helper functions
 */

void request_new_worker() {
    int tag = random();
    Request_msg req(tag);
    req.set_arg("name", "my worker");
    request_new_worker_node(req);
    mstate.requested_workers++;
}


int work_estimate(Request_msg& req) {
    std::string job = req.get_arg("cmd");
    int estimation;

    if (job == "418wisdom") {
        estimation = 175;
    } else if (job == "projectidea") {
        estimation = 3 * 14 / sizeof(void *);
    } else if (job == "tellmenow") {
        estimation = 1;
    } else if (job == "countprimes") {
        estimation = (int) ceil(atoi(req.get_arg("n").c_str()) / 100000.0);
    } else {
        estimation = 0;
        DLOG(INFO) << "Work estimation: invalid job name." << std::endl;
    }

    return estimation;
}


Worker_handle find_best_receiver(Request_msg& req) {
    for (auto &pair : mstate.worker_roster) {
        Worker_handle worker = pair.first;
        Worker_state& wstate = pair.second;

        if (req.get_arg("cmd") == "projectidea") {
            if (!wstate.processing_cached_job[0] &&
                    wstate.work_estimate[1] == 0) {
                set_thread_id(req, 1);
                return worker;
            } else if (!wstate.processing_cached_job[1] &&
                    wstate.work_estimate[2] == 0) {
                set_thread_id(req, 2);
                return worker;
            }
        } else {
            int start_thread = (worker == mstate.first_worker) ? 1 : 0;
            for (int i = start_thread; i < NUM_THREADS; i++) {
                if (i == 1 || i == 2) continue;
                if (wstate.work_estimate[i] == 0) {
                    set_thread_id(req, i);
                    return worker;
                }
            }
        }
    }
    return NULL;
}


void distribute_job(Request_msg& req) {
    int tag = req.get_tag() / 100;

    // if instant job, send to the first worker on thread 0 directly
    if (req.get_arg("cmd") == "tellmenow") {
        Worker_handle job_receiver = mstate.first_worker;
        Worker_state& wstate = mstate.worker_roster[job_receiver];
        set_thread_id(req, 0);
        wstate.instant_job_count++;
        wstate.work_estimate[0]++;
        wstate.idle_time = 0;
        send_request_to_worker(job_receiver, req);
    // if it is an cached job
    } else if (req.get_arg("cmd") == "projectidea") {
        Worker_handle job_receiver = find_best_receiver(req);
        if (job_receiver == NULL) {
            mstate.pending_cached_jobs.push(tag);
        } else {
            Worker_state& wstate = mstate.worker_roster[job_receiver];
            wstate.processing_cached_job[get_thread_id(req) - 1] = true;
            wstate.job_count++;
            wstate.idle_time = 0;
            wstate.work_estimate[get_thread_id(req)] += work_estimate(req);
            send_request_to_worker(job_receiver, req);
        }
    }
    // other jobs (418wisdom, countprimes)
    else {
        Worker_handle job_receiver = find_best_receiver(req);
        if (job_receiver == NULL) {
            mstate.pending_requests.push(tag);
        } else {
            Worker_state& wstate = mstate.worker_roster[job_receiver];
            wstate.job_count++;
            wstate.idle_time = 0;
            wstate.work_estimate[get_thread_id(req)] += work_estimate(req);
            send_request_to_worker(job_receiver, req);
        }
    }
}


void distribute_job_to_worker(Worker_handle worker, Request_msg& req) {
    Worker_state& wstate = mstate.worker_roster[worker];
    if (req.get_arg("cmd") == "tellmenow") {
        set_thread_id(req, 0);
        wstate.instant_job_count++;
        wstate.work_estimate[0]++;
        wstate.idle_time = 0;
        send_request_to_worker(worker, req);
    } else if (req.get_arg("cmd") == "projectidea") {
        wstate.processing_cached_job[get_thread_id(req) - 1] = true;
        wstate.job_count++;
        wstate.idle_time = 0;
        wstate.work_estimate[get_thread_id(req)] += work_estimate(req);
        send_request_to_worker(worker, req);
    } else {
        wstate.job_count++;
        wstate.idle_time = 0;
        wstate.work_estimate[get_thread_id(req)] += work_estimate(req);
        send_request_to_worker(worker, req);
    }
}


void compute_cmp_prime_resp(
        Response_msg& cmp_prime_resp,
        std::vector<Response_msg> prime_resp) {

    int counts[4];
    for (int i = 0; i < 4; i++) {
        counts[i] = atoi(prime_resp[i].get_response().c_str());
    }

    if (counts[1] - counts[0] > counts[3] - counts[2])
        cmp_prime_resp.set_response("There are more primes in first range.");
    else
        cmp_prime_resp.set_response("There are more primes in second range.");
}
