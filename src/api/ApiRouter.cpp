#include "api/ApiRouter.hpp"
#include <algorithm>
#include <iostream>
#include <stdexcept>

void ApiRouter::registerHandler(int16_t api_key, int16_t min_version, int16_t max_version, std::unique_ptr<IApiHandler> handler) {
    handlers[api_key] = std::move(handler);
    api_versions.push_back({api_key, min_version, max_version});

    // List sorted by API key for consistent responses
    std::sort(api_versions.begin(), api_versions.end(), [](const auto &a, const auto &b) { return a.api_key < b.api_key; });
}

std::vector<ApiVersionInfo> ApiRouter::getSupportedApis() const {
    return api_versions;
}

kafka::protocol::Response ApiRouter::routeRequest(const kafka::protocol::Request &request) {
    auto it = handlers.find(request.api_key);
    if (it != handlers.end()) {
        return it->second->handle(request);
    }

    std::cerr << "No handler found for API key: " << request.api_key << std::endl;
    kafka::protocol::Response error_response(request.correlation_id);
    error_response.writeInt16(3); // UNSUPPORTED_VERSION error
    return error_response;
}
