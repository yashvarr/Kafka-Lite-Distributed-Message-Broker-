#include "protocol/Protocol.hpp"
#include "protocol/BufferReader.hpp"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>
#include <iostream>

namespace
{
    // Helper to read a specific number of bytes
    bool recv_all(int sock_fd, char *buf, size_t len)
    {
        size_t total = 0;
        while (total < len)
        {
            ssize_t n = recv(sock_fd, buf + total, len - total, 0);
            if (n <= 0)
                return false;
            total += n;
        }
        return true;
    }

    template <typename T>
    T read_big_endian(const char *&data)
    {
        T value;
        memcpy(&value, data, sizeof(T));
        data += sizeof(T);
        if constexpr (sizeof(T) == 2)
            return ntohs(value);
        if constexpr (sizeof(T) == 4)
            return ntohl(value);
        if constexpr (sizeof(T) == 8)
            return be64toh(value); // For 64-bit
        return value;
    }

    std::string read_string(const char *&data)
    {
        int16_t len = read_big_endian<int16_t>(data);
        if (len <= 0)
            return "";
        std::string s(data, len);
        data += len;
        return s;
    }
}

namespace kafka::protocol
{

    std::vector<char> read_message(int socket_fd)
    {
        char size_buf[4];
        if (!recv_all(socket_fd, size_buf, 4))
        {
            return {}; // Connection closed
        }
        int32_t message_size = ntohl(*reinterpret_cast<int32_t *>(size_buf));

        if (message_size > 10 * 1024 * 1024)
        { // 10MB sanity limit
            throw std::runtime_error("Message size too large: " + std::to_string(message_size));
        }

        std::vector<char> buffer(message_size);
        if (!recv_all(socket_fd, buffer.data(), message_size))
        {
            throw std::runtime_error("Failed to read full message body");
        }
        return buffer;
    }

    void send_message(int socket_fd, const std::vector<char> &message)
    {
        ssize_t sent = send(socket_fd, message.data(), message.size(), 0);
        if (sent != static_cast<ssize_t>(message.size()))
        {
            throw std::runtime_error("Failed to send complete message");
        }
    }

    Request parse_request(const std::vector<char> &data)
    {
        Request req;
        BufferReader reader(data);

        req.api_key = reader.readInt16();
        req.api_version = reader.readInt16();
        req.correlation_id = reader.readInt32();
        req.client_id = reader.readString();

        reader.readInt8(); // Skip the tagged fields count byte.

        // The rest of the buffer is the API-specific body.
        size_t body_size = reader.bytes_remaining();
        const char *body_start = data.data() + (data.size() - body_size);
        req.body.assign(body_start, body_start + body_size);
        return req;
    }

    std::vector<char> serialize_response(const Response &response)
    {
        // Total size = 4 (message size) + 4 (correlation id) + payload size
        int32_t payload_size = response.get_data().size() + 4; // Add 4 for correlation ID
        int32_t total_size = payload_size + 4;

        std::vector<char> buffer;
        buffer.reserve(total_size);

        // 1. Message Size
        int32_t msg_size_be = htonl(payload_size);
        const char *p = reinterpret_cast<const char *>(&msg_size_be);
        buffer.insert(buffer.end(), p, p + sizeof(msg_size_be));

        // 2. Correlation ID
        int32_t correlation_id_be = htonl(response.get_correlation_id());
        p = reinterpret_cast<const char *>(&correlation_id_be);
        buffer.insert(buffer.end(), p, p + sizeof(correlation_id_be));

        // 3. Payload
        const auto &payload = response.get_data();
        buffer.insert(buffer.end(), payload.begin(), payload.end());

        return buffer;
    }

} // namespace kafka::protocol