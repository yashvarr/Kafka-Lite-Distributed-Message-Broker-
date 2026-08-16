#include "core/ThreadPool.hpp"

ThreadPool::ThreadPool(size_t num_threads) : stop(false)
{
    for (size_t i = 0; i < num_threads; ++i)
    {
        workers.emplace_back([this]
                             { this->worker_thread(); });
    }
}

void ThreadPool::worker_thread()
{
    while (true)
    {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(this->queue_mutex);
            // Wait until there's a task or the pool is stopped
            this->condition.wait(lock, [this]
                                 { return this->stop || !this->tasks.empty(); });

            if (this->stop && this->tasks.empty())
            {
                return;
            }

            task = std::move(this->tasks.front());
            this->tasks.pop();
        }
        task();
    }
}

void ThreadPool::enqueue(std::function<void()> task)
{
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        if (stop)
        {
            throw std::runtime_error("enqueue on stopped ThreadPool");
        }
        tasks.emplace(std::move(task));
    }
    condition.notify_one();
}

ThreadPool::~ThreadPool()
{
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for (std::thread &worker : workers)
    {
        worker.join();
    }
}