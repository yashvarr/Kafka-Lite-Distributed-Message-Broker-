#pragma once
#include <memory>
#include "core/ThreadPool.hpp"
#include "api/ApiRouter.hpp"

class Server
{
public:
    Server(int port, std::shared_ptr<ThreadPool> pool, std::shared_ptr<ApiRouter> router);
    ~Server();

    void start();

private:
    void setup_socket();
    void accept_loop();

    int port;
    int server_fd;
    std::shared_ptr<ThreadPool> thread_pool;
    std::shared_ptr<ApiRouter> api_router;
};