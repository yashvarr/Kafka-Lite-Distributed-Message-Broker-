#pragma once
#include <vector>
#include <queue>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>

class ThreadPool
{
public:
    ThreadPool(size_t num_threads);
    ~ThreadPool();

    void enqueue(std::function<void()> task);

private:
    void worker_thread();

    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};