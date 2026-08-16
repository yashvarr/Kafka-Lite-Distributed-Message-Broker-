#pragma once
#include <vector>
#include <string>
#include <cstdint>
#include <stdexcept>
#include <arpa/inet.h>
#include <cstring>

namespace kafka::protocol
{

    class BufferReader
    {
    public:
        BufferReader(const char *data, size_t size);
        BufferReader(const std::vector<char> &buffer);

        int8_t readInt8();
        int16_t readInt16();
        int32_t readInt32();
        int64_t readInt64();
        std::string readString();
        std::string readString(size_t len);
        std::vector<uint8_t> readBytes(size_t len);

        void skip(size_t bytes);
        bool eof() const; // End of file/buffer
        size_t bytes_remaining() const { return m_size - m_pos; }

    private:
        void ensure_can_read(size_t bytes);

        const char *m_data;
        size_t m_size;
        size_t m_pos;
    };

} // namespace kafka::protocol