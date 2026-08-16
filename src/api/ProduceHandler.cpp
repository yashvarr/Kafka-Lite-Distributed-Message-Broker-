#include "api/ProduceHandler.hpp"
#include "protocol/BufferReader.hpp"
#include "storage/IMetadataStore.hpp"
#include <chrono>

ProduceHandler::ProduceHandler(std::shared_ptr<IMetadataStore> store)
    : metadata_store(store) {}

kafka::protocol::Response ProduceHandler::handle(const kafka::protocol::Request &request) {
    kafka::protocol::BufferReader reader(request.body);
    kafka::protocol::Response response(request.correlation_id);

    try {
        // Parse request
        int8_t transactional_id_len = reader.readInt8();
        if (transactional_id_len > 0) {
            reader.skip(transactional_id_len - 1);
        }

        int16_t acks = reader.readInt16();
        int32_t timeout_ms = reader.readInt32();

        int8_t topic_count = reader.readInt8() - 1;

        // Response header
        response.writeInt8(0);               // top-level tagged fields
        response.writeInt8(topic_count + 1); // responses array size

        for (int i = 0; i < topic_count; ++i) {
            int8_t topic_name_len = reader.readInt8() - 1;
            std::string topic_name = reader.readString(topic_name_len);
            reader.readInt8(); // tag buffer

            response.writeString(topic_name);

            int8_t partition_count = reader.readInt8() - 1;
            response.writeInt8(partition_count + 1);

            for (int j = 0; j < partition_count; ++j) {
                int32_t partition_id = reader.readInt32();

                // Skip record batch for now - simplified implementation
                int32_t record_batch_size = reader.readInt32();
                if (record_batch_size > 0) {
                    reader.skip(record_batch_size);
                }

                reader.readInt8(); // tag buffer

                // Get topic UUID
                std::vector<uint8_t> topic_uuid = metadata_store->get_topic_uuid(topic_name);
                bool topic_exists = metadata_store->is_topic_known(topic_name);

                response.writeInt32(partition_id);

                if (!topic_exists) {
                    response.writeInt16(3);  // UNKNOWN_TOPIC_OR_PARTITION
                    response.writeInt64(-1); // base_offset
                    response.writeInt64(-1); // log_append_time
                    response.writeInt64(-1); // log_start_offset
                } else {
                    // Append a dummy message
                    std::vector<uint8_t> key, value;
                    int64_t offset = metadata_store->append_message(topic_uuid, partition_id, key, value, {});

                    response.writeInt16(0);      // NO_ERROR
                    response.writeInt64(offset); // base_offset

                    int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::system_clock::now().time_since_epoch())
                                            .count();
                    response.writeInt64(timestamp); // log_append_time
                    response.writeInt64(0);         // log_start_offset
                }

                response.writeInt8(1);    // record_errors (empty)
                response.writeString(""); // error_message
                response.writeInt8(0);    // tag buffer
            }

            response.writeInt8(0); // topic tag buffer
        }

        response.writeInt32(0); // throttle_time_ms
        response.writeInt8(0);  // final tag buffer
    } catch (const std::exception &e) {
        response.writeInt16(35); // UNSUPPORTED_VERSION
    }

    return response;
}
