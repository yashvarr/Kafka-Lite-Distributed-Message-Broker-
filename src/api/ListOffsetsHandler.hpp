#pragma once
#include "api/IApiHandler.hpp"
#include <memory>

class IMetadataStore;

class ListOffsetsHandler : public IApiHandler {
public:
    explicit ListOffsetsHandler(std::shared_ptr<IMetadataStore> metadata_store);
    kafka::protocol::Response handle(const kafka::protocol::Request &request) override;

private:
    std::shared_ptr<IMetadataStore> metadata_store;
};
