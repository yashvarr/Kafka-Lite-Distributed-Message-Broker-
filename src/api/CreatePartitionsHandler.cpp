#include "api/CreatePartitionsHandler.hpp"
#include "protocol/BufferReader.hpp"
#include "storage/IMetadataStore.hpp"

CreatePartitionsHandler::CreatePartitionsHandler(std::shared_ptr<IMetadataStore> store)
    : metadata_store(store) {}

kafka::protocol::Response CreatePartitionsHandler::handle(const kafka::protocol::Request &request) {
    kafka::protocol::BufferReader reader(request.body);
    kafka::protocol::Response response(request.correlation_id);

    try {
        // Parse request
        int8_t topic_count = reader.readInt8() - 1;

        std::vector<std::pair<std::string, int32_t>> topics_to_expand;

        for (int i = 0; i < topic_count; ++i) {
            int8_t topic_name_len = reader.readInt8() - 1;
            std::string topic_name = reader.readString(topic_name_len);

            int32_t new_partition_count = reader.readInt32();

            // Skip assignments array
            int8_t assignments_len = reader.readInt8();
            if (assignments_len > 1) {
                for (int j = 0; j < assignments_len - 1; ++j) {
                    int8_t broker_len = reader.readInt8();
                    for (int k = 0; k < broker_len - 1; ++k) {
                        reader.readInt32(); // broker_id
                    }
                    reader.readInt8(); // tag buffer
                }
            }

            reader.readInt8(); // topic tag buffer

            topics_to_expand.push_back({topic_name, new_partition_count});
        }

        int32_t timeout_ms = 5000;
        bool validate_only = false;

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
        response.writeInt8(topics_to_expand.size() + 1);

        for (const auto &[topic_name, new_partition_count] : topics_to_expand) {
            response.writeString(topic_name);

            if (validate_only) {
                response.writeInt16(0);   // NO_ERROR
                response.writeString(""); // error_message
            } else {
                bool exists = metadata_store->is_topic_known(topic_name);

                if (!exists) {
                    response.writeInt16(3); // UNKNOWN_TOPIC_OR_PARTITION
                    response.writeString("Topic does not exist");
                } else {
                    bool expanded = metadata_store->create_partitions(topic_name, new_partition_count);

                    if (expanded) {
                        response.writeInt16(0);   // NO_ERROR
                        response.writeString(""); // error_message
                    } else {
                        response.writeInt16(37); // INVALID_PARTITIONS
                        response.writeString("Cannot decrease partition count or partition count unchanged");
                    }
                }
            }

            response.writeInt8(0); // topic tag buffer
        }

        response.writeInt8(0); // final tag buffer
    } catch (const std::exception &e) {
        response.writeInt16(35); // UNSUPPORTED_VERSION
    }

    return response;
}
