#pragma once
#include "api/IApiHandler.hpp"
#include <memory>

class IMetadataStore;

class ProduceHandler : public IApiHandler {
public:
    explicit ProduceHandler(std::shared_ptr<IMetadataStore> metadata_store);
    kafka::protocol::Response handle(const kafka::protocol::Request &request) override;

private:
    std::shared_ptr<IMetadataStore> metadata_store;
};
