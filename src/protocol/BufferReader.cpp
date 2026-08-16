#include "protocol/BufferReader.hpp"

namespace kafka::protocol
{

    BufferReader::BufferReader(const char *data, size_t size)
        : m_data(data), m_size(size), m_pos(0) {}

    BufferReader::BufferReader(const std::vector<char> &buffer)
        : m_data(buffer.data()), m_size(buffer.size()), m_pos(0) {}

    void BufferReader::ensure_can_read(size_t bytes)
    {
        if (m_pos + bytes > m_size)
        {
            throw std::runtime_error("Attempt to read past end of buffer");
        }
    }

    int8_t BufferReader::readInt8()
    {
        ensure_can_read(1);
        return m_data[m_pos++];
    }

    int16_t BufferReader::readInt16()
    {
        ensure_can_read(2);
        int16_t val;
        memcpy(&val, m_data + m_pos, 2);
        m_pos += 2;
        return ntohs(val);
    }

    int32_t BufferReader::readInt32()
    {
        ensure_can_read(4);
        int32_t val;
        memcpy(&val, m_data + m_pos, 4);
        m_pos += 4;
        return ntohl(val);
    }

    int64_t BufferReader::readInt64()
    {
        ensure_can_read(8);
        int64_t val;
        memcpy(&val, m_data + m_pos, 8);
        m_pos += 8;
        return static_cast<int64_t>(be64toh(static_cast<uint64_t>(val)));
    }

    std::string BufferReader::readString()
    {
        int16_t len = readInt16();
        if (len < 0)
        { // Null string
            return "";
        }
        ensure_can_read(len);
        std::string s(m_data + m_pos, len);
        m_pos += len;
        return s;
    }

    std::string BufferReader::readString(size_t len)
    {
        if (len < 0)
        { // Null string
            return "";
        }
        ensure_can_read(len);
        std::string s(m_data + m_pos, len);
        m_pos += len;
        return s;
    }

    std::vector<uint8_t> BufferReader::readBytes(size_t len)
    {
        ensure_can_read(len);
        const uint8_t *start = reinterpret_cast<const uint8_t *>(m_data + m_pos);
        std::vector<uint8_t> bytes(start, start + len);
        m_pos += len;
        return bytes;
    }

    void BufferReader::skip(size_t bytes)
    {
        ensure_can_read(bytes);
        m_pos += bytes;
    }

    bool BufferReader::eof() const
    {
        return m_pos >= m_size;
    }

} // namespace kafka::protocol