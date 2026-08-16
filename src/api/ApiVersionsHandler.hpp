#pragma once
#include "api/ApiRouter.hpp"
#include "api/IApiHandler.hpp"
#include <memory>

class ApiVersionsHandler : public IApiHandler {
public:
    explicit ApiVersionsHandler(std::shared_ptr<ApiRouter> router);

    kafka::protocol::Response handle(const kafka::protocol::Request &request) override;

private:
    // A weak_ptr is used to prevent circular dependencies/ownership cycles.
    std::weak_ptr<ApiRouter> router;
};
