#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>

#include "argparse.hpp"

using namespace std;
using namespace argparse;


#define SMALL 1


struct Saver {
    int id;
    long long bytes_used;
    Saver(int id, long long bytes_used) {
        this->id = id;
        this->bytes_used = bytes_used;
    }
};


struct StatusCompare {
    bool operator() (const Saver& l, const Saver& r) {
        return l.bytes_used > r.bytes_used;
    }
};

enum class Action {
    Read,
    Write,
    End,
};

struct Message {
    string requestId;
    Action action;
    string objectId;
    long long size;
    string hash;
    Message() {}
    Message(const string& rid, Action act, const string& oid, long long sz=0ll, const string& h="") {
        requestId = rid;
        action = act;
        objectId = oid;
        size = sz;
        hash = h;
    }
};


typedef queue<Message> TaskQueue;
typedef queue<string> ResultQueue;


void SaverFn(TaskQueue& task, ResultQueue& result_queue, mutex& m, mutex& result_mutex, const int id) {
    unordered_map<string, string> _store;
    string name = to_string(id) + ".log";
    //ofstream file(name);
    //cerr << " Thread: " << id << " executing" << endl;
    while (true) {
        if (task.empty()) {
            usleep(1); // will sleep for 0.001 ms
            continue;
        }
        Message message;
        string result;
        {
            lock_guard<mutex> l(m);
            message = task.front();
            task.pop();
        }
        if (message.action == Action::Read) {
            auto hash = _store[message.objectId];
            //file << message.requestId << "," << id << "|" << result << endl;
            //file << message.requestId << "," << result << endl;
            result += message.requestId;
            result.append(",");
            result += hash;
        } else if (message.action == Action::Write) {
            //file << message.requestId << "," << id << endl;
            result += message.requestId;
            result.append(",");
            result += to_string(id);
            _store[message.objectId] = message.hash;
        } else {
            //cerr << "Receive stop signal" << endl;
            break;
        }
        {
            lock_guard<mutex> l(result_mutex);
            result_queue.push(result);
        }
    }
}

void OutputFn(ResultQueue& result, mutex& m) {
#ifdef SMALL
        string name = "output_small.txt";
#else
        string name = "output.txt";
#endif
    //ofstream output(name.c_str());

    //cerr << "Writing to file: " << name << endl;
    while (true) {
        if (result.empty()) {
            usleep(1); // will sleep for 0.001 ms
            continue;
        }
        string r;
        {
            lock_guard<mutex> l(m);
            r = result.front();
            result.pop();
        }
        if (r.compare("ghost_bye_bye") == 0) {
            break;
        }
        //output << r << endl;
        cout << r << endl;
    }
}


class Router {
private:
    priority_queue<Saver, vector<Saver>, StatusCompare > _saver;
    vector<TaskQueue*> _tasks;
    ResultQueue _result_queue;
    unordered_map<string, int> _meta;
    vector<thread> ts;
    vector<mutex> _m;
    mutex _result;
    int _num;
    Saver GetSaver() {
        // Make sure _saver is not empty
        auto top = _saver.top();
        _saver.pop();
        return top;
    }
    void InsertSaver(const Saver& saver) {
        _saver.push(saver);
    }
public:
    Router(int num): _m(num) {
        _num = num;
        for (int i = 0; i < num; ++i) {
            _saver.push(Saver(i, 0));
            TaskQueue* q = new queue<Message>();
            _tasks.push_back(q);
            ts.emplace_back(
                SaverFn,
                std::ref(*_tasks.back()),
                std::ref(_result_queue),
                std::ref(_m[i]),
                std::ref(_result),
                i);
        }
        ts.emplace_back(OutputFn, std::ref(_result_queue), std::ref(_result));
    }
    ~Router() {
        for (int i = 0; i < _num; ++i) {
            //cerr << "Send stop signal for " << i << "\n";
            {
                lock_guard<mutex> l(_m[i]); // useless
                (*_tasks[i]).push(Message("", Action::End, ""));
            }
            ts[i].join();
            // Make sure everything is empty here.
            //cerr << "Size: " << _tasks[i]->size() << endl;
            delete _tasks[i];
        }

        _result_queue.push("ghost_bye_bye");
        ts.back().join();
    }
    void Save(const string& requestId, const string& objectId, long long bytes, const string& hash) {
        Saver s = GetSaver();
        _meta[objectId] = s.id;
        s.bytes_used += bytes;
        InsertSaver(s);
        //cerr << "Saving for " << objectId << " on " << s.id << endl;
        lock_guard<mutex> l(_m[s.id]);
        (*_tasks[s.id]).push(Message(requestId, Action::Write, objectId, bytes, hash));
    }

    void Read(const string& requestId, const string& objectId) {
        int id = _meta[objectId];
        lock_guard<mutex> l(_m[id]);
        (*_tasks[id]).push(Message(requestId, Action::Read, objectId));
    }
    void Stat() {
        cerr << "Saver size: " << _saver.size() << endl;
        while (!_saver.empty()) {
            auto s = _saver.top();
            _saver.pop();
            cerr << "Bucket id = " << s.id << " bytes = " << s.bytes_used << endl;
        }
    }
};


int Process(char* buf, const int bufsize, Router& router, int& counter) {
    int start = 0;
    vector<string> res;
    while (start < bufsize) {
        int line_end = start;
        while (line_end < bufsize && buf[line_end] != '\n') {
            ++line_end;
        }
        if (line_end >= bufsize) {
            return bufsize - start;
        }
        //cerr << "Line: " << string(buf, line_end - start) << endl;
        int line_start = start;
        while (line_start < line_end) {
            int sep_end = line_start;
            while (sep_end < line_end && buf[sep_end] != ',') ++sep_end;
            res.emplace_back(buf+line_start, sep_end - line_start);
            line_start = sep_end + 1;
            //cerr << "res content: " << res.back() << endl;
        }
        //cerr << "DEBUG: res size = " << res.size() << endl;
        if (res.size() == 3) {
            // Read
            router.Read(res[0], res[2]);
        } else {
            // Write
            router.Save(res[0], res[2], stoll(res[3]), res[4]);
        }
        res.clear();
        start = line_end + 1;
        counter += 1;
        if (counter % 1000000 == 0) {
            cerr << "Handled number = " << counter << endl;
        }
    }
    return 0;
}



int main(int argc, char* argv[]) {
    ArgumentParser parser("Bucket router");
    parser.add_argument("-h", "--help")
        .action([=](const std::string& s) {
                std::cout << parser.help().str();
        })
        .default_value(false)
        .help("shows help message")
        .implicit_value(true)
        .nargs(0);
    parser.add_argument("-n")
       .scan<'d', int>();
    parser.add_argument("--data");
    parser.parse_args(argc, argv);
    const int num = parser.get<int>("-n");
    string name = parser.get<string>("--data");
    ifstream input(name.c_str(), ios::binary);
    cerr << "Bucket num = " << num << " Data file = " << name << endl;
    Router l(num);
    string line;
    string delimiter = ",";
    const int TenMB = 819200 + 1;
    int shift = 0;
    int start = 0;
    int counter = 0;
    char buf[TenMB];
    auto start_time = chrono::system_clock::now();
    auto timenow = chrono::system_clock::to_time_t(start_time);
    cerr << "Start time = " << ctime(&timenow) << endl;
    while (true) {
        input.read(buf + shift, TenMB - shift);
        int gcount = input.gcount();
        //cerr << "Gc count: " << gcount << " shift: " << shift << endl;
        if (gcount == 0) {
            break;
        }
        shift = Process(buf, shift + gcount, l, counter);
        if (shift != 0) {
            strcpy(buf, buf + TenMB - shift);
        }
    }
    l.Stat();
    auto finish_time = chrono::system_clock::now();
    timenow = chrono::system_clock::to_time_t(finish_time);
    cerr << "Finish time = " << ctime(&timenow) << endl;
    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(finish_time - start_time);
    cerr << "Time cost in seconds = " << milliseconds.count() / 1000.0 << endl;
    return EXIT_SUCCESS;
}
