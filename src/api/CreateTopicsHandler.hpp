#pragma once
#include "api/IApiHandler.hpp"
#include <memory>

class IMetadataStore;

class CreateTopicsHandler : public IApiHandler {
public:
    explicit CreateTopicsHandler(std::shared_ptr<IMetadataStore> metadata_store);
    kafka::protocol::Response handle(const kafka::protocol::Request &request) override;

private:
    std::shared_ptr<IMetadataStore> metadata_store;
};
