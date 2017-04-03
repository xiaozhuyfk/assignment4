#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <sstream>
#include <glog/logging.h>

//#include <iostream>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include "tools/work_queue.h"

#define MAX_THREADS_NUM 23 // leave one for tellmenow

WorkQueue<Request_msg> work_queue;
WorkQueue<Request_msg> tell_queue;

// For the use of pthread
static void* worker_thread(void* arg) {
   while (1) {
     Request_msg req = work_queue.get_work();
     Response_msg resp(req.get_tag());
     execute_work(req, resp);
     worker_send_response(resp);
   }
   return NULL;
}

static void* tell_thread(void* arg) {
   while (1) {
     Request_msg req = tell_queue.get_work();
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

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";

  // One thread here as the master thread
  for (int i=0; i<MAX_THREADS_NUM; i++) {
    pthread_t t;
    pthread_create(&t, NULL, &worker_thread, NULL);
  }
  // leave one high way for tellmenow
  pthread_t t;
  pthread_create(&t, NULL, &tell_thread, NULL);
}

void worker_handle_request(const Request_msg& req) {

  // Output debugging help to the logs (in a single worker node
  // configuration, this would be in the log logs/worker.INFO)
  DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

  if (req.get_arg("cmd").compare("tellmenow") == 0) {
    tell_queue.put_work(req);
  } else {
    work_queue.put_work(req);
  }
}
