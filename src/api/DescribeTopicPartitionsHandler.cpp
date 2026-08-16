#include "api/DescribeTopicPartitionsHandler.hpp"
#include "protocol/BufferReader.hpp"
#include "storage/IMetadataStore.hpp"

DescribeTopicPartitionsHandler::DescribeTopicPartitionsHandler(std::shared_ptr<IMetadataStore> store)
    : metadata_store(store) {}

kafka::protocol::Response DescribeTopicPartitionsHandler::handle(const kafka::protocol::Request &request) {
    kafka::protocol::BufferReader reader(request.body);
    kafka::protocol::Response response(request.correlation_id);

    try {
        // Parse the request body
        std::vector<std::string> requested_topics;

        int32_t topic_array_size = reader.readInt8() - 1;
        for (int32_t i = 0; i < topic_array_size; ++i) {
            int topic_id_size = reader.readInt8() - 1;
            std::string topic_name = reader.readString(topic_id_size);
            requested_topics.push_back(topic_name);
            reader.readInt8(); // Tag buffer
        }

        int32_t response_partition_limit = 2000;
        if (!reader.eof()) {
            response_partition_limit = reader.readInt32();
        }

        // Cursor handling (simplified - skip for now)
        if (!reader.eof()) {
            int8_t cursor_len = reader.readInt8();
            if (cursor_len > 0) {
                reader.skip(cursor_len - 1);
            }
        }

        reader.readInt8(); // final tag buffer

        // Build response
        response.writeInt8(0);  // top-level tagged fields
        response.writeInt32(0); // throttle_time_ms

        response.writeInt8(requested_topics.size() + 1); // topics array size

        for (const auto &topic_name : requested_topics) {
            bool is_known = metadata_store->is_topic_known(topic_name);

            response.writeInt16(is_known ? 0 : 3); // error_code
            response.writeString(topic_name);

            std::vector<uint8_t> topic_id = metadata_store->get_topic_uuid(topic_name);
            response.writeBytes(topic_id);

            response.writeInt8(0); // is_internal

            if (is_known) {
                auto topic_info = metadata_store->get_topic_info(topic_name);
                auto partitions_data = metadata_store->get_serialized_partitions(topic_id);

                response.writeInt8(partitions_data.size() + 1); // partitions array size
                for (const auto &part_data : partitions_data) {
                    response.writeBytes(part_data);
                }
            } else {
                response.writeInt8(1); // empty partitions array
            }

            response.writeInt32(0x00000df8); // topic_authorized_operations
            response.writeInt8(0);           // topic tagged fields
        }

        response.writeInt8(0xFF); // next_cursor (null)
        response.writeInt8(0);    // final tagged fields
    } catch (const std::exception &e) {
        response.writeInt16(35); // UNSUPPORTED_VERSION
    }

    return response;
}
