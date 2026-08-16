#pragma once
#include "protocol/Request.hpp"
#include "protocol/Response.hpp"
#include <vector>
#include <cstdint>

namespace kafka::protocol
{
    // Reads a complete Kafka message (size-prefixed) from a socket.
    std::vector<char> read_message(int socket_fd);

    // Writes a complete Kafka message to a socket.
    void send_message(int socket_fd, const std::vector<char> &message);

    // Parses a byte buffer into a Request object.
    Request parse_request(const std::vector<char> &data);

    // Serializes a Response object into a byte buffer for sending.
    std::vector<char> serialize_response(const Response &response);
}