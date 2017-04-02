#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <assert.h>

#include <map>
#include <vector>
#include <queue>

#include "server/messages.h"
#include "server/master.h"
#include "tools/work_queue.h"


#define NUM_THREADS 24

struct Worker_state {
        int job_count;
        int idle_time;
        bool processing_cached_job;
        std::vector<int> work_estimate;
};

static struct Master_state {

        // The mstate struct collects all the master node state into one
        // place.  You do not need to preserve any of the fields below, they
        // exist only to implement the basic functionality of the starter
        // code.

        bool server_ready;
        int max_num_workers;
        int num_pending_client_requests;
        int next_tag;

        Worker_handle my_worker;
        Client_handle waiting_client;

        // all workers alive
        std::map<Worker_handle, Worker_state> worker_roster;

        // tag-client map
        std::map<int, Client_handle> client_mapping;

        // tag-request map
        std::map<int, Request_msg> request_mapping;

        // queue of idle workers (no current jobs)
        std::queue<Worker_handle> idle_workers;

        // queue of requests that not assigned to workers
        std::queue<int> pending_requests;

} mstate;

void master_node_init(int max_workers, int& tick_period) {

    tick_period = 1;

    mstate.next_tag = 0;
    mstate.max_num_workers = max_workers;
    mstate.num_pending_client_requests = 0;

    // don't mark the server as ready until the server is ready to go.
    // This is actually when the first worker is up and running, not
    // when 'master_node_init' returnes
    mstate.server_ready = false;

    // fire off a request for a new worker

    for (size_t i = 0; i < max_workers; i++) {
        int tag = random();
        Request_msg req(tag);
        req.set_arg("name", "my worker");
        request_new_worker_node(req);
    }

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

    // 'tag' allows you to identify which worker request this response
    // corresponds to.  Since the starter code only sends off one new
    // worker request, we don't use it here.

    mstate.worker_roster[worker_handle].job_count = 0;
    mstate.worker_roster[worker_handle].idle_time = 0;
    mstate.worker_roster[worker_handle].processing_cached_job = false;
    mstate.worker_roster[worker_handle].work_estimate =
            std::vector<int>(NUM_THREADS, 0);
    mstate.idle_workers.push(worker_handle);

    // Now that a worker is booted, let the system know the server is
    // ready to begin handling client requests.  The test harness will
    // now start its timers and start hitting your server with requests.
    if (mstate.server_ready == false) {
        server_init_complete();
        mstate.server_ready = true;
    }
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
    } else if (job == "compareprimes") {
        int n1 = (int) ceil(atoi(req.get_arg("n1").c_str()) / 100000.0);
        int n2 = (int) ceil(atoi(req.get_arg("n2").c_str()) / 100000.0);
        int n3 = (int) ceil(atoi(req.get_arg("n3").c_str()) / 100000.0);
        int n4 = (int) ceil(atoi(req.get_arg("n4").c_str()) / 100000.0);
        estimation = n1 + n2 + n3 + n4;
    } else {
        estimation = 0;
        DLOG(WARNING) << "Work estimation: invalid job name." << std::endl;
    }

    return estimation;
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

    int tag = resp.get_tag();
    int thread_id = resp.get_thread_id();
    Request_msg req = mstate.request_mapping[tag];
    Client_handle client = mstate.client_mapping[tag];
    Worker_state wstate = mstate.worker_roster[worker_handle];
    std::string job = req.get_arg("cmd");

    if (job == "projectidea") {
        wstate.processing_cached_job = false;
    }

    if (job != "tellmenow") wstate.job_count--;
    wstate.work_estimate[thread_id] -= work_estimate(req);
    assert(wstate.work_estimate[thread_id] >= 0);

    send_client_response(client, resp);
    mstate.client_mapping.erase(tag);
    mstate.request_mapping.erase(tag);

    if (wstate.job_count == 0) {
        mstate.idle_workers.push(worker_handle);
    }
}

Worker_handle find_best_receiver(Request_msg& req) {
    int estimation = work_estimate(req);
    int min_max = INT_MAX;
    Worker_handle receiver = NULL;

    for (auto const &pair : mstate.worker_roster) {
        Worker_handle worker = pair.first;
        Worker_state wstate = pair.second;

        if (wstate.processing_cached_job) continue;

        int min_estimation = INT_MAX;
        int min_thread_id = 1;
        int max_estimation = 0;
        for (int i = 1; i < NUM_THREADS; i++) {
            if (wstate.work_estimate[i] < min_estimation) {
                min_estimation = wstate.work_estimate[i];
                min_thread_id = i;
            }
            if (wstate.work_estimate[i] > max_estimation)
                max_estimation = wstate.work_estimate[i];
        }

        if (std::max(max_estimation, min_estimation + estimation) < min_max) {
            receiver = worker;
            req.set_thread_id(min_thread_id);
        }
    }

    return receiver;
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
    int tag = mstate.next_tag++;
    Request_msg worker_req(tag, client_req);
    mstate.client_mapping[tag] = client_handle;
    mstate.request_mapping[tag] = worker_req;

    if (worker_req.get_arg("cmd") == "tellmenow") {
        int min_instant_job_count = INT_MAX;
        Worker_handle job_receiver;
        Worker_state wstate;
        for (auto const &pair : mstate.worker_roster) {
            if (pair.second.work_estimate[0] < min_instant_job_count) {
                min_instant_job_count = pair.second.work_estimate[0];
                job_receiver = pair.first;
                wstate = pair.second;
            }
        }

        worker_req.set_thread_id(0);
        wstate.work_estimate[0]++;
        wstate.idle_time = 0;
        send_request_to_worker(job_receiver, worker_req);
    } else if (worker_req.get_arg("cmd") == "projectidea") {
        if (mstate.idle_workers.size() == 0) {
            if (mstate.worker_roster.size() < mstate.max_num_workers) {
                int tag = random();
                Request_msg req(tag);
                req.set_arg("name", "my worker 0");
                request_new_worker_node(req);
            }
            mstate.pending_requests.push(tag);
        } else {
            Worker_handle job_receiver = mstate.idle_workers.front();
            mstate.idle_workers.pop();
            Worker_state wstate = mstate.worker_roster[job_receiver];
            wstate.processing_cached_job = true;
            wstate.job_count++;
            wstate.idle_time = 0;
            wstate.work_estimate[1] += work_estimate(worker_req);
            worker_req.set_thread_id(1);
            send_request_to_worker(job_receiver, worker_req);
        }
    } else {
        Worker_handle job_receiver = find_best_receiver(worker_req);
        if (job_receiver == NULL) {
            mstate.pending_requests.push(tag);
        } else {
            Worker_state wstate = mstate.worker_roster[job_receiver];
            wstate.job_count++;
            wstate.idle_time = 0;
            wstate.work_estimate[worker_req.get_thread_id()] +=
                    work_estimate(worker_req);
            send_request_to_worker(job_receiver, worker_req);
        }
    }

    // We're done!  This event handler now returns, and the master
    // process calls another one of your handlers when action is
    // required.

}

void handle_tick() {

    // TODO: you may wish to take action here.  This method is called at
    // fixed time intervals, according to how you set 'tick_period' in
    // 'master_node_init'.

    while (mstate.pending_requests.size() > 0) {
        int tag = mstate.pending_requests.front();
        Request_msg req = mstate.request_mapping[tag];
        if (req.get_arg("cmd") == "projectidea") {
            if (mstate.idle_workers.size() == 0) {
                break;
            } else {
                Worker_handle job_receiver = mstate.idle_workers.front();
                mstate.idle_workers.pop();
                Worker_state wstate = mstate.worker_roster[job_receiver];
                wstate.processing_cached_job = true;
                wstate.job_count++;
                wstate.idle_time = 0;
                wstate.work_estimate[1] += work_estimate(req);
                req.set_thread_id(1);
                send_request_to_worker(job_receiver, req);
                mstate.pending_requests.pop();
            }
        } else {
            Worker_handle job_receiver = find_best_receiver(req);
            if (job_receiver == NULL) {
                break;
            } else {
                Worker_state wstate = mstate.worker_roster[job_receiver];
                wstate.job_count++;
                wstate.idle_time = 0;
                wstate.work_estimate[req.get_thread_id()] += work_estimate(req);
                send_request_to_worker(job_receiver, req);
                mstate.pending_requests.pop();
            }
        }
    }

}

