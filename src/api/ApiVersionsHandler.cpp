#include "api/ApiVersionsHandler.hpp"
#include <stdexcept>

ApiVersionsHandler::ApiVersionsHandler(std::shared_ptr<ApiRouter> router) : router(router) {}

kafka::protocol::Response ApiVersionsHandler::handle(const kafka::protocol::Request &request) {
    kafka::protocol::Response response(request.correlation_id);

    // Lock the weak_ptr to get a shared_ptr.
    auto router_ptr = router.lock();
    if (!router_ptr) {
        // This should never happen in a running server, but it's safe to handle.
        throw std::runtime_error("ApiRouter is not available");
    }

    // Version of the ApiVersions request itself. We only support versions 0-4.
    if (request.api_version < 0 || request.api_version > 4) {
        response.writeInt16(35); // UNSUPPORTED_VERSION
        return response;
    }

    // Building the response
    response.writeInt16(0); // ErrorCode: NONE

    // Get the list of supported APIs directly from the router
    const auto supported_apis = router_ptr->getSupportedApis();
    response.writeInt8(supported_apis.size() + 1);

    // Loop through the APIs and add them to the response
    for (const auto &api_info : supported_apis) {
        response.writeInt16(api_info.api_key);
        response.writeInt16(api_info.min_version);
        response.writeInt16(api_info.max_version);

        // For ApiVersions >= v3, there is a tagged fields section.
        if (request.api_version >= 3) {
            response.writeInt8(0); // Number of tagged fields.
        }
    }

    response.writeInt32(0); // ThrottleTimeMs

    // For ApiVersions >= v3, there is a final tagged fields section.
    if (request.api_version >= 3) {
        response.writeInt8(0); // Number of tagged fields.
    }

    return response;
}
