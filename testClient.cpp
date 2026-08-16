#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <vector>
#include <string>

// Helper to append big-endian integers
template <typename T>
void append_big_endian(std::vector<uint8_t> &buf, T value) {
    for (int i = sizeof(T) - 1; i >= 0; --i) {
        buf.push_back((value >> (i * 8)) & 0xFF);
    }
}

int main() {
    const char *server_ip = "127.0.0.1";
    int server_port = 9092; // Change if needed

    // You can have up to 5 topics here
    std::vector<std::string> topics = { "topic1", "topic2", "topic3", "topic4", "topic5" };

    // Build request body
    std::vector<uint8_t> body;

    // API key (2 bytes)
    append_big_endian<int16_t>(body, 75); // Example API key
    // API version (2 bytes)
    append_big_endian<int16_t>(body, 0);
    // Correlation ID (4 bytes)
    append_big_endian<int32_t>(body, 123);

    // Client ID
    std::string client_id = "test-client";
    append_big_endian<uint16_t>(body, client_id.size());
    body.insert(body.end(), client_id.begin(), client_id.end());

    // Topic tag buffer (1 byte)
    body.push_back(0);

    // Topics array length + 1 (1 byte)
    body.push_back(static_cast<uint8_t>(topics.size() + 1));

    // Each topic
    for (auto &topic : topics) {
        body.push_back(static_cast<uint8_t>(topic.size() + 1)); // length+1
        body.insert(body.end(), topic.begin(), topic.end());    // topic name
        body.push_back(0); // topic tag buffer
    }

    // Response partition limit (4 bytes)
    append_big_endian<int32_t>(body, 10);
    // Cursor (1 byte)
    body.push_back(0);
    // Final tag buffer (1 byte)
    body.push_back(0);

    // Wrap with message size (4 bytes prefix)
    std::vector<uint8_t> message;
    append_big_endian<int32_t>(message, body.size());
    message.insert(message.end(), body.begin(), body.end());

    // Connect to server
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

    if (connect(sock, (sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    // Send the message
    if (send(sock, message.data(), message.size(), 0) != (ssize_t)message.size()) {
        perror("send");
        close(sock);
        return 1;
    }

    std::cout << "Request sent (" << message.size() << " bytes) with " << topics.size() << " topics.\n";

    // Receive response (optional)
    uint8_t response[1024];
    ssize_t n = recv(sock, response, sizeof(response), 0);
    if (n > 0) {
        std::cout << "Received " << n << " bytes from server\n";
    }

    close(sock);
    return 0;
}

