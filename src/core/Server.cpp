#include "core/Server.hpp"
#include "protocol/Protocol.hpp"
#include <iostream>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <vector>

// Forward declaration for the connection handler
void handle_connection(int client_fd, std::shared_ptr<ApiRouter> router);

Server::Server(int port, std::shared_ptr<ThreadPool> pool, std::shared_ptr<ApiRouter> router)
    : port(port), server_fd(-1), thread_pool(pool), api_router(router) {}

Server::~Server()
{
    if (server_fd != -1)
    {
        close(server_fd);
    }
}

void Server::start()
{
    setup_socket();
    accept_loop();
}

void Server::setup_socket()
{
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        throw std::runtime_error("Failed to create server socket");
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
        close(server_fd);
        throw std::runtime_error("setsockopt failed");
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr), sizeof(server_addr)) != 0)
    {
        close(server_fd);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port));
    }

    if (listen(server_fd, 10) != 0)
    {
        close(server_fd);
        throw std::runtime_error("listen failed");
    }
}

void Server::accept_loop()
{
    std::cout << "Waiting for connections...\n";
    while (true)
    {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0)
        {
            std::cerr << "accept failed\n";
            continue;
        }
        std::cout << "Client connected\n";

        // Pass the router to the handler task
        thread_pool->enqueue([client_fd, router = this->api_router]
                             { handle_connection(client_fd, router); });
    }
}

// This function now replaces the old `kafkaConnection` logic.
void handle_connection(int client_fd, std::shared_ptr<ApiRouter> router)
{
    try
    {
        while (true)
        {
            // Read the full request from the socket
            std::vector<char> request_bytes = kafka::protocol::read_message(client_fd);
            if (request_bytes.empty())
            {
                break; // Client disconnected
            }

            // Parse the request bytes
            kafka::protocol::Request request = kafka::protocol::parse_request(request_bytes);

            // Route to the correct API handler to get a response
            kafka::protocol::Response response = router->routeRequest(request);

            // Serialize the response and send it back
            std::vector<char> response_bytes = kafka::protocol::serialize_response(response);
            kafka::protocol::send_message(client_fd, response_bytes);
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error handling connection: " << e.what() << std::endl;
    }

    std::cout << "Client disconnected\n";
    close(client_fd);
}