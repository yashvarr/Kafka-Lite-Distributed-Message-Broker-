#include "api/FetchHandler.hpp"
#include "protocol/BufferReader.hpp"
#include "storage/IMetadataStore.hpp"
#include <arpa/inet.h>
#include <array>
#include <cstdint>
#include <memory>
#include <vector>

struct PartitionFetchInfo {
    int32_t index;
    int32_t current_leader_epoch;
    int64_t fetch_offset;
    int32_t last_fetched_epoch;
    int64_t log_start_offset;
    int32_t partition_max_bytes;
};

struct TopicFetchInfo {
    std::array<uint8_t, 16> id;
    std::vector<PartitionFetchInfo> partitions;
};

class FetchRequestData {
public:
    int32_t max_wait_ms;
    int32_t min_bytes;
    int32_t max_bytes;
    int8_t isolation_level;
    int32_t session_id;
    int32_t session_epoch;
    std::vector<TopicFetchInfo> topics;

    static FetchRequestData parse(kafka::protocol::BufferReader &reader) {
        FetchRequestData request_data;

        request_data.max_wait_ms = reader.readInt32();
        request_data.min_bytes = reader.readInt32();
        request_data.max_bytes = reader.readInt32();
        request_data.isolation_level = reader.readInt8();
        request_data.session_id = reader.readInt32();
        request_data.session_epoch = reader.readInt32();

        int32_t topic_array_size = reader.readInt8() - 1;

        if (topic_array_size > 0) {
            request_data.topics.reserve(topic_array_size);
        }

        for (int i = 0; i < topic_array_size; ++i) {
            TopicFetchInfo current_topic;

            std::vector<uint8_t> topic_id_vec = reader.readBytes(16);
            if (topic_id_vec.size() == 16) {
                std::copy(topic_id_vec.begin(), topic_id_vec.end(), current_topic.id.begin());
            }

            int32_t partition_array_size = reader.readInt8() - 1;
            if (partition_array_size > 0) {
                current_topic.partitions.reserve(partition_array_size);
            }

            for (int j = 0; j < partition_array_size; ++j) {
                current_topic.partitions.emplace_back(PartitionFetchInfo{
                    .index = reader.readInt32(),
                    .current_leader_epoch = reader.readInt32(),
                    .fetch_offset = reader.readInt64(),
                    .last_fetched_epoch = reader.readInt32(),
                    .log_start_offset = reader.readInt64(),
                    .partition_max_bytes = reader.readInt32()});

                reader.readInt8(); // partition tag buffer
            }

            reader.readInt8(); // topic tag buffer
            request_data.topics.push_back(std::move(current_topic));
        }

        reader.readInt8(); // forgotten_topics_data
        reader.readInt8(); // rack_id
        reader.readInt8(); // final tag buffer

        return request_data;
    }
};

FetchHandler::FetchHandler(std::shared_ptr<IMetadataStore> store)
    : metadata_store(store) {}

kafka::protocol::Response FetchHandler::handle(const kafka::protocol::Request &request) {
    kafka::protocol::BufferReader reader(request.body);
    kafka::protocol::Response response(request.correlation_id);

    try {
        FetchRequestData request_data = FetchRequestData::parse(reader);

        response.writeInt8(0);  // top-level tagged fields
        response.writeInt32(0); // throttle_time_ms
        response.writeInt16(0); // error_code
        response.writeInt32(0); // session_id

        if (request_data.topics.empty()) {
            response.writeInt8(1); // num_responses = 0, encoded as 1
            response.writeInt8(0); // Final tag buffer
            return response;
        }

        response.writeInt8(request_data.topics.size() + 1); // num_responses

        for (const auto &topic : request_data.topics) {
            std::vector<uint8_t> topic_id_vec(topic.id.begin(), topic.id.end());
            response.writeBytes(topic_id_vec);

            response.writeInt8(topic.partitions.size() + 1); // partitions array size

            for (const auto &partition : topic.partitions) {
                response.writeInt32(partition.index); // partition index

                bool is_known = metadata_store->is_uuid_known(topic_id_vec);

                if (is_known) {
                    response.writeInt16(0); // error_code: NO_ERROR

                    // Fetch messages
                    auto messages = metadata_store->fetch_messages(
                        topic_id_vec,
                        partition.index,
                        partition.fetch_offset,
                        partition.partition_max_bytes);

                    int64_t high_watermark = metadata_store->get_latest_offset(topic_id_vec, partition.index);
                    int64_t log_start_offset = metadata_store->get_earliest_offset(topic_id_vec, partition.index);

                    response.writeInt64(high_watermark);   // high_watermark
                    response.writeInt64(high_watermark);   // last_stable_offset
                    response.writeInt64(log_start_offset); // log_start_offset

                    response.writeInt8(1);   // aborted_transactions (empty)
                    response.writeInt32(-1); // preferred_read_replica

                    // Build simplified record batch
                    if (!messages.empty()) {
                        std::vector<uint8_t> record_batch;

                        // Record batch header (simplified)
                        int64_t base_offset = messages[0].offset;
                        int32_t batch_length = 61 + messages.size() * 20; // Simplified

                        int64_t base_offset_be = htobe64(base_offset);
                        record_batch.insert(record_batch.end(),
                                            reinterpret_cast<uint8_t *>(&base_offset_be),
                                            reinterpret_cast<uint8_t *>(&base_offset_be) + 8);

                        int32_t batch_length_be = htonl(batch_length);
                        record_batch.insert(record_batch.end(),
                                            reinterpret_cast<uint8_t *>(&batch_length_be),
                                            reinterpret_cast<uint8_t *>(&batch_length_be) + 4);

                        // Simplified batch content
                        for (size_t i = 0; i < 49; ++i)
                            record_batch.push_back(0);

                        int32_t record_count_be = htonl(messages.size());
                        record_batch.insert(record_batch.end(),
                                            reinterpret_cast<uint8_t *>(&record_count_be),
                                            reinterpret_cast<uint8_t *>(&record_count_be) + 4);

                        // Add message data (simplified)
                        for (const auto &msg : messages) {
                            record_batch.insert(record_batch.end(), msg.value.begin(), msg.value.end());
                        }

                        response.writeInt8(2); // records array size
                        response.writeBytes(record_batch);
                    } else {
                        response.writeInt8(1); // Empty record_set
                    }
                } else {
                    response.writeInt16(100); // error_code: UNKNOWN_TOPIC_ID
                    response.writeInt64(-1);  // high_watermark
                    response.writeInt64(-1);  // last_stable_offset
                    response.writeInt64(-1);  // log_start_offset
                    response.writeInt8(1);    // aborted_transactions (empty)
                    response.writeInt32(-1);  // preferred_read_replica
                    response.writeInt8(1);    // Empty record_set
                }

                response.writeInt8(0); // partition tag buffer
            }

            response.writeInt8(0); // topic tag buffer
        }

        response.writeInt8(0); // Final tag buffer
    } catch (const std::exception &e) {
        response.writeInt16(35); // UNSUPPORTED_VERSION
    }

    return response;
}