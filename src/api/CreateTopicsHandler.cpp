#include "api/CreateTopicsHandler.hpp"
#include "protocol/BufferReader.hpp"
#include "storage/IMetadataStore.hpp"

CreateTopicsHandler::CreateTopicsHandler(std::shared_ptr<IMetadataStore> store)
    : metadata_store(store) {}

kafka::protocol::Response CreateTopicsHandler::handle(const kafka::protocol::Request &request) {
    kafka::protocol::BufferReader reader(request.body);
    kafka::protocol::Response response(request.correlation_id);

    try {
        // Parse request
        int8_t topic_count = reader.readInt8() - 1;

        int32_t timeout_ms = 5000;
        bool validate_only = false;

        // Skip to topics
        std::vector<std::tuple<std::string, int32_t, int16_t>> topics_to_create;

        for (int i = 0; i < topic_count; ++i) {
            int8_t topic_name_len = reader.readInt8() - 1;
            std::string topic_name = reader.readString(topic_name_len);

            int32_t num_partitions = reader.readInt32();
            int16_t replication_factor = reader.readInt16();

            // Skip assignments array
            int8_t assignments_len = reader.readInt8();
            if (assignments_len > 1) {
                for (int j = 0; j < assignments_len - 1; ++j) {
                    reader.readInt32(); // partition_id
                    int8_t broker_len = reader.readInt8();
                    for (int k = 0; k < broker_len - 1; ++k) {
                        reader.readInt32(); // broker_id
                    }
                    reader.readInt8(); // tag buffer
                }
            }

            // Skip configs array
            int8_t configs_len = reader.readInt8();
            if (configs_len > 1) {
                for (int j = 0; j < configs_len - 1; ++j) {
                    int8_t config_name_len = reader.readInt8() - 1;
                    reader.skip(config_name_len); // config name
                    int8_t config_value_len = reader.readInt8();
                    if (config_value_len > 0) {
                        reader.skip(config_value_len - 1);
                    }
                    reader.readInt8(); // tag buffer
                }
            }

            reader.readInt8(); // topic tag buffer

            topics_to_create.push_back({topic_name, num_partitions, replication_factor});
        }

        // Read timeout and validate_only if present
        if (!reader.eof()) {
            timeout_ms = reader.readInt32();
        }
        if (!reader.eof()) {
            validate_only = reader.readInt8() != 0;
        }

        // Response header
        response.writeInt8(0);  // top-level tagged fields
        response.writeInt32(0); // throttle_time_ms

        // Topics response
        response.writeInt8(topics_to_create.size() + 1);

        for (const auto &[topic_name, num_partitions, replication_factor] : topics_to_create) {
            response.writeString(topic_name);

            if (validate_only) {
                response.writeInt16(0);   // NO_ERROR
                response.writeString(""); // error_message
            } else {
                bool created = metadata_store->create_topic(topic_name, num_partitions, replication_factor);

                if (created) {
                    response.writeInt16(0);   // NO_ERROR
                    response.writeString(""); // error_message
                } else {
                    response.writeInt16(36); // TOPIC_ALREADY_EXISTS
                    response.writeString("Topic already exists");
                }
            }

            std::vector<uint8_t> topic_uuid = metadata_store->get_topic_uuid(topic_name);
            response.writeBytes(topic_uuid);

            response.writeInt32(num_partitions);     // num_partitions
            response.writeInt16(replication_factor); // replication_factor

            response.writeInt8(1); // configs (empty)
            response.writeInt8(0); // topic tag buffer
        }

        response.writeInt8(0); // final tag buffer
    } catch (const std::exception &e) {
        response.writeInt16(35); // UNSUPPORTED_VERSION
    }

    return response;
}
