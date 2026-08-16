#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace kafka::protocol
{

    struct Request
    {
        int16_t api_key;
        int16_t api_version;
        int32_t correlation_id;
        std::string client_id;

        // The unparsed request body, for the specific handler to process.
        std::vector<char> body;
    };

} // namespace kafka::protocol