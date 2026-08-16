#include "api/ListOffsetsHandler.hpp"
#include "protocol/BufferReader.hpp"
#include "storage/IMetadataStore.hpp"

ListOffsetsHandler::ListOffsetsHandler(std::shared_ptr<IMetadataStore> store)
    : metadata_store(store) {}

kafka::protocol::Response ListOffsetsHandler::handle(const kafka::protocol::Request &request) {
    kafka::protocol::BufferReader reader(request.body);
    kafka::protocol::Response response(request.correlation_id);

    try {
        // Parse request
        int32_t replica_id = reader.readInt32();
        int8_t isolation_level = reader.readInt8();

        int8_t topic_count = reader.readInt8() - 1;

        // Response header
        response.writeInt8(0);               // top-level tagged fields
        response.writeInt32(0);              // throttle_time_ms
        response.writeInt8(topic_count + 1); // topics array size

        for (int i = 0; i < topic_count; ++i) {
            int8_t topic_name_len = reader.readInt8() - 1;
            std::string topic_name = reader.readString(topic_name_len);

            response.writeString(topic_name);

            int8_t partition_count = reader.readInt8() - 1;
            response.writeInt8(partition_count + 1);

            for (int j = 0; j < partition_count; ++j) {
                int32_t partition_id = reader.readInt32();

                // Read timestamp (can be -2 for earliest, -1 for latest, or actual timestamp)
                int64_t timestamp = reader.readInt64();

                reader.readInt8(); // tag buffer

                std::vector<uint8_t> topic_uuid = metadata_store->get_topic_uuid(topic_name);
                bool topic_exists = metadata_store->is_topic_known(topic_name);

                response.writeInt32(partition_id);

                if (!topic_exists) {
                    response.writeInt16(3);  // UNKNOWN_TOPIC_OR_PARTITION
                    response.writeInt64(-1); // timestamp
                    response.writeInt64(-1); // offset
                } else {
                    int64_t offset;
                    if (timestamp == -2) {
                        // Earliest offset
                        offset = metadata_store->get_earliest_offset(topic_uuid, partition_id);
                    } else if (timestamp == -1) {
                        // Latest offset
                        offset = metadata_store->get_latest_offset(topic_uuid, partition_id);
                    } else {
                        // Specific timestamp
                        offset = metadata_store->get_offset_by_timestamp(topic_uuid, partition_id, timestamp);
                    }

                    response.writeInt16(0);         // NO_ERROR
                    response.writeInt64(timestamp); // timestamp
                    response.writeInt64(offset);    // offset
                }

                response.writeInt32(-1); // leader_epoch (optional)
                response.writeInt8(0);   // tag buffer
            }

            response.writeInt8(0); // topic tag buffer
        }

        response.writeInt8(0); // final tag buffer
    } catch (const std::exception &e) {
        response.writeInt16(35); // UNSUPPORTED_VERSION
    }

    return response;
}
