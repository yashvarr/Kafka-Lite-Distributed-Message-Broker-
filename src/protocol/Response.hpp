#pragma once
#include <vector>
#include <cstdint>
#include <string>
#include <arpa/inet.h>

namespace kafka::protocol
{

    class Response
    {
    public:
        Response(int32_t correlation_id);

        // Endian-safe writers
        void writeInt8(int8_t val);
        void writeInt16(int16_t val);
        void writeInt32(int32_t val);
        void writeInt64(int64_t val);
        void writeString(const std::string &s);
        void writeBytes(const std::vector<uint8_t> &bytes);
        void writeRawBytes(const char *data, size_t len);

        int32_t get_correlation_id() const { return correlation_id; }
        const std::vector<char> &get_data() const { return data; }

    private:
        int32_t correlation_id;
        std::vector<char> data;
    };

} // namespace kafka::protocol