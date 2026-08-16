#pragma once
#include "api/IApiHandler.hpp"
#include <memory>

class IMetadataStore;

class FetchHandler : public IApiHandler {
public:
    // We use dependency injection to provide the data store.
    explicit FetchHandler(std::shared_ptr<IMetadataStore> metadata_store);

    kafka::protocol::Response handle(const kafka::protocol::Request &request) override;

private:
    std::shared_ptr<IMetadataStore> metadata_store;
};
