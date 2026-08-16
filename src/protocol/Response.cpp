#include "protocol/Response.hpp"
#include <cstring>
#include <cstdint>
#include <endian.h>

namespace kafka::protocol
{

    Response::Response(int32_t correlation_id) : correlation_id(correlation_id) {}

    void Response::writeInt8(int8_t val)
    {
        data.push_back(val);
    }

    void Response::writeInt16(int16_t val)
    {
        int16_t be_val = htons(val);
        const char *p = reinterpret_cast<const char *>(&be_val);
        data.insert(data.end(), p, p + sizeof(be_val));
    }

    void Response::writeInt32(int32_t val)
    {
        int32_t be_val = htonl(val);
        const char *p = reinterpret_cast<const char *>(&be_val);
        data.insert(data.end(), p, p + sizeof(be_val));
    }

    void Response::writeInt64(int64_t val)
    {
        int64_t be_val = htobe64(val);
        const char *p = reinterpret_cast<const char *>(&be_val);
        data.insert(data.end(), p, p + sizeof(be_val));
    }

    void Response::writeString(const std::string &s)
    {
        writeInt8(s.length() + 1);
        data.insert(data.end(), s.begin(), s.end());
    }

    void Response::writeBytes(const std::vector<uint8_t> &bytes)
    {
        data.insert(data.end(), bytes.begin(), bytes.end());
    }

    void Response::writeRawBytes(const char *raw_data, size_t len)
    {
        data.insert(data.end(), raw_data, raw_data + len);
    }

} // namespace kafka::protocol