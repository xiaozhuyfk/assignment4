#ifndef __LIBASST4_MESSAGES_H__
#define __LIBASST4_MESSAGES_H__

#include <map>
#include <string>

class Request_msg {

    private:
        std::map<std::string, std::string> dict;
        std::string request_str;
        int tag;
        int thread_id;

    public:
        Request_msg() {
            tag = 0;
            thread_id = 0;
        }
        Request_msg(int tag);
        Request_msg(int tag, const std::string& str);
        Request_msg(int tag, const Request_msg& j);
        Request_msg(const Request_msg& j); // copy constructor

        std::string get_arg(const std::string& name) const;
        void set_arg(const std::string& key, const std::string& value);

        void set_tag(int arg_tag) {
            tag = arg_tag * 100 + (tag % 100);
        }
        int get_tag() const {
            return tag / 100;
        }

        void set_thread_id(int id) {
            tag = (tag / 100) * 100 + id;
        }
        int get_thread_id() const {
            return tag % 100;
        }

        std::string get_request_string() const;
};

class Response_msg {

    private:
        int tag;
        int thread_id;
        std::string resp_str;

    public:

        Response_msg() {
            tag = 0;
            thread_id = 0;
        }

        Response_msg(int arg_tag) {
            tag = arg_tag;
            thread_id = 0;
        }

        void set_tag(int arg_tag) {
            tag = arg_tag * 100 + (tag % 100);
        }
        int get_tag() const {
            return tag / 100;
        }

        void set_thread_id(int id) {
            tag = (tag / 100) * 100 + id;
        }
        int get_thread_id() const {
            return tag % 100;
        }

        std::string get_response() const {
            return resp_str;
        }

        void set_response(const std::string& str) {
            resp_str = str;
        }
};

#endif
