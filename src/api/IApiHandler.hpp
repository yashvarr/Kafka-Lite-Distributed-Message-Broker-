#pragma once
#include "protocol/Request.hpp"
#include "protocol/Response.hpp"

// Interface for all API handlers
class IApiHandler {
public:
    virtual ~IApiHandler() = default;
    virtual kafka::protocol::Response handle(const kafka::protocol::Request &request) = 0;
};
