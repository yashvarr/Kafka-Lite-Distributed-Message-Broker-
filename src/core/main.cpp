#include "api/ApiRouter.hpp"
#include "api/ApiVersionsHandler.hpp"
#include "api/CreatePartitionsHandler.hpp"
#include "api/CreateTopicsHandler.hpp"
#include "api/DescribeTopicPartitionsHandler.hpp"
#include "api/FetchHandler.hpp"
#include "api/ListOffsetsHandler.hpp"
#include "api/MetadataHandler.hpp"
#include "api/ProduceHandler.hpp"
#include "core/Server.hpp"
#include "core/ThreadPool.hpp"
#include "storage/InMemoryMetadataStore.hpp"
#include <iostream>
#include <memory>

int main(int argc, char *argv[]) {
    // Unbuffer output for immediate logging
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    const int port = 9092;
    const int num_threads = 4;

    try {
        // Setup the data store (using in-memory store for simplicity)
        auto metadataStore = std::make_shared<InMemoryMetadataStore>();
        std::cout << "Initialized in-memory metadata store.\n";

        // Create some default topics for testing
        metadataStore->create_topic("test-topic", 3, 1);
        metadataStore->create_topic("quickstart-events", 1, 1);
        std::cout << "Created default topics.\n";

        // Setup the API routing logic
        auto apiRouter = std::make_shared<ApiRouter>();

        // Register all API handlers with their version ranges
        apiRouter->registerHandler(0, 0, 11, std::make_unique<ProduceHandler>(metadataStore));
        apiRouter->registerHandler(1, 0, 16, std::make_unique<FetchHandler>(metadataStore));
        apiRouter->registerHandler(2, 0, 8, std::make_unique<ListOffsetsHandler>(metadataStore));
        apiRouter->registerHandler(3, 0, 13, std::make_unique<MetadataHandler>(metadataStore));

        // ApiVersionsHandler needs the router to report available APIs
        auto apiVersionsHandler = std::make_unique<ApiVersionsHandler>(apiRouter);
        apiRouter->registerHandler(18, 0, 4, std::move(apiVersionsHandler));

        apiRouter->registerHandler(19, 0, 7, std::make_unique<CreateTopicsHandler>(metadataStore));
        apiRouter->registerHandler(37, 0, 3, std::make_unique<CreatePartitionsHandler>(metadataStore));
        apiRouter->registerHandler(75, 0, 0, std::make_unique<DescribeTopicPartitionsHandler>(metadataStore));

        // Setup the thread pool
        auto threadPool = std::make_shared<ThreadPool>(num_threads);
        std::cout << "Thread pool with " << num_threads << " workers created.\n";

        // Start the server
        Server server(port, threadPool, apiRouter);
        std::cout << "Server starting on port " << port << "...\n";
        std::cout << "Ready to accept connections.\n";
        server.start();
    } catch (const std::exception &e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}