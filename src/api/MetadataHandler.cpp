#include "api/MetadataHandler.hpp"
#include "protocol/BufferReader.hpp"
#include "storage/IMetadataStore.hpp"

MetadataHandler::MetadataHandler(std::shared_ptr<IMetadataStore> store)
    : metadata_store(store) {}

kafka::protocol::Response MetadataHandler::handle(const kafka::protocol::Request &request) {
    kafka::protocol::BufferReader reader(request.body);
    kafka::protocol::Response response(request.correlation_id);

    try {
        // Parse request
        int8_t topic_count = reader.readInt8();
        std::vector<std::string> requested_topics;

        bool request_all_topics = (topic_count == 0);

        if (!request_all_topics) {
            topic_count -= 1;
            for (int i = 0; i < topic_count; ++i) {
                int8_t topic_name_len = reader.readInt8() - 1;
                std::string topic_name = reader.readString(topic_name_len);
                requested_topics.push_back(topic_name);
                reader.readInt8(); // tag buffer
            }
        }

        bool allow_auto_topic_creation = false;
        if (!reader.eof()) {
            allow_auto_topic_creation = reader.readInt8() != 0;
        }

        // Response header
        response.writeInt8(0);  // top-level tagged fields
        response.writeInt32(0); // throttle_time_ms

        // Brokers
        response.writeInt8(2);  // 1 broker + 1
        response.writeInt32(0); // broker id
        response.writeString("localhost");
        response.writeInt32(9092); // port
        response.writeString("");  // rack
        response.writeInt8(0);     // broker tag buffer

        response.writeString(""); // cluster_id
        response.writeInt32(0);   // controller_id

        // Topics
        std::vector<std::string> topics_to_return;
        if (request_all_topics) {
            topics_to_return = metadata_store->get_all_topic_names();
        } else {
            topics_to_return = requested_topics;
        }

        response.writeInt8(topics_to_return.size() + 1);

        for (const auto &topic_name : topics_to_return) {
            response.writeInt16(0); // error_code
            response.writeString(topic_name);

            std::vector<uint8_t> topic_uuid = metadata_store->get_topic_uuid(topic_name);
            response.writeBytes(topic_uuid);

            response.writeInt8(0); // is_internal

            auto topic_info = metadata_store->get_topic_info(topic_name);

            if (topic_info.partitions.empty()) {
                response.writeInt8(1); // empty partitions array
            } else {
                response.writeInt8(topic_info.partitions.size() + 1);

                for (const auto &[part_id, partition] : topic_info.partitions) {
                    response.writeInt16(0); // error_code
                    response.writeInt32(partition.partition_id);
                    response.writeInt32(partition.leader);
                    response.writeInt32(partition.leader_epoch);

                    response.writeInt8(partition.replicas.size() + 1);
                    for (int32_t replica : partition.replicas) {
                        response.writeInt32(replica);
                    }

                    response.writeInt8(partition.isr.size() + 1);
                    for (int32_t isr : partition.isr) {
                        response.writeInt32(isr);
                    }

                    response.writeInt8(1); // offline_replicas (empty)
                    response.writeInt8(0); // partition tag buffer
                }
            }

            response.writeInt32(0x00000df8); // topic_authorized_operations
            response.writeInt8(0);           // topic tag buffer
        }

        response.writeInt8(0); // final tag buffer
    } catch (const std::exception &e) {
        response.writeInt16(35); // UNSUPPORTED_VERSION
    }

    return response;
}
