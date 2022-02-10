#include <cstdlib>
#include <string>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <queue>
#include <unordered_map>
#include <chrono>
#include <thread>
#include <mutex>

using namespace std;


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


void SaverFn(queue<Message>& task, mutex& m, const int id) {
    unordered_map<string, string> _store;
    string name = to_string(id) + ".log";
    ofstream file(name);
    //cout << " Thread: " << id << " executing" << endl;
    while (true) {
        if (task.empty()) {
            usleep(1); // will sleep for 0.001 ms
            continue;
        }
        Message message;
        {
            lock_guard<mutex> l(m);
            message = task.front();
            task.pop();
        }
        if (message.action == Action::Read) {
            auto result = _store[message.objectId];
            file << message.requestId << "," << id << "|" << result << endl;
        } else if (message.action == Action::Write) {
            file << message.requestId << "," << message.objectId << endl;
            _store[message.objectId] = message.hash;
        } else {
            //cout << "Receive stop signal" << endl;
            break;
        }
    }
}

typedef queue<Message> Q;

class Router {
private:
    priority_queue<Saver, vector<Saver>, StatusCompare > _saver;
    vector<Q> _tasks;
    unordered_map<string, int> _meta;
    vector<thread> ts;
    mutex _m;
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
    Router(int num) {
        _num = num;
        for (int i = 0; i < num; ++i) {
            _saver.push(Saver(i, 0));
            _tasks.push_back(queue<Message>());
            std::thread t(SaverFn, std::ref(_tasks[_tasks.size()-1]), std::ref(_m), i);
            ts.push_back(std::move(t));
        }
    }
    ~Router() {
        for (int i = 0; i < _num; ++i) {
            //cout << "Send stop signal for " << i << "\n";
            (_tasks[i]).push(Message("", Action::End, ""));
            ts[i].join();
            //delete _tasks[i];
        }
    }
    void Save(const string& requestId, const string& objectId, long long bytes, const string& hash) {
        Saver s = GetSaver();
        _meta[objectId] = s.id;
        s.bytes_used += bytes;
        InsertSaver(s);
        //cout << "Saving for " << objectId << " on " << s.id << endl;
        lock_guard<mutex> l(_m);
        (_tasks[s.id]).push(Message(requestId, Action::Write, objectId, bytes, hash));
    }

    void Read(const string& requestId, const string& objectId) {
        int id = _meta[objectId];
        lock_guard<mutex> l(_m);
        (_tasks[id]).push(Message(requestId, Action::Read, objectId));
    }
    void Stat() {
        cout << "Saver size: " << _saver.size() << endl;
        while (!_saver.empty()) {
            auto s = _saver.top();
            _saver.pop();
            cout << "Bucket id = " << s.id << " bytes = " << s.bytes_used << endl;
        }
    }
};


int main(int argc, char* argv[]) {
    Router l(2);
    ifstream input("data.txt");
    string line;
    string delimiter = ",";
    while (getline(input, line)) {
        auto s = 0;
        auto e = line.find(delimiter);
        vector<string> res;
        //cout << "Line: " << line << endl;
        while (e != string::npos) {
            res.push_back(line.substr(s, e-s));
            s = e + delimiter.length();
            e = line.find(delimiter, s);
            //cout << "s: " << s << " e: " << e << " Size: " << line.size() << endl;
        }
        res.push_back(line.substr(s));
        if (res.size() == 3) {
            // Read
            l.Read(res[0], res[2]);
        } else {
            // Write
            l.Save(res[0], res[2], stoll(res[3]), res[4]);
        }
    }
    l.Save("0", "i902", 38, "zzkj");
    l.Read("0", "i902");
    l.Stat();
    return EXIT_SUCCESS;
}

