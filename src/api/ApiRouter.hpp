#pragma once
#include "IApiHandler.hpp"
#include <map>
#include <memory>
#include <vector>

// A simple struct to hold the metadata for each API
struct ApiVersionInfo {
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
};

class ApiRouter {
public:
    void registerHandler(int16_t api_key, int16_t min_version, int16_t max_version, std::unique_ptr<IApiHandler> handler);
    kafka::protocol::Response routeRequest(const kafka::protocol::Request &request);

    std::vector<ApiVersionInfo> getSupportedApis() const;

private:
    std::map<int16_t, std::unique_ptr<IApiHandler>> handlers;
    std::vector<ApiVersionInfo> api_versions; // API Metadata store
};
