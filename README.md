# mini-Kafka
Lightweight Kafka-inspired message broker built from scratch which supports basic publish/subscribe, topic management, and message persistence.

## Core Features

-   **Multi-threaded TCP Server:** Handles multiple client connections concurrently.
-   **Kafka Protocol Compliant:** Correctly handles request/response framing and big-endian byte order.
-   **Extensible Design:** Built with a clean, decoupled architecture to make adding new API handlers simple.

### Building

A bash script is provided which will compile and run the Kafka server.

**Requirements**
- CMAKE
- GCC version 13 or above

Build and run:
```sh
./run.sh
```

Run only with speciific folders:
```sh
./build/kafka <folder name>
```

## How to Extend (Add a New API)

The project is designed for easy extension. To add support for a new Kafka API:

1.  **Create a Handler:** Add a new class in `src/api/` that inherits from `IApiHandler`.
2.  **Implement Logic:** In its `handle()` method, use the `BufferReader` to parse the request body and the `Response` builder to construct the reply.
3.  **Register Handler:** In `src/core/main.cpp`, register your new handler with the `ApiRouter`, providing its API key and supported versions.
