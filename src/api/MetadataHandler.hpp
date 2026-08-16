#pragma once
#include "api/IApiHandler.hpp"
#include <memory>

class IMetadataStore;

class MetadataHandler : public IApiHandler {
public:
    explicit MetadataHandler(std::shared_ptr<IMetadataStore> metadata_store);
    kafka::protocol::Response handle(const kafka::protocol::Request &request) override;

private:
    std::shared_ptr<IMetadataStore> metadata_store;
};
