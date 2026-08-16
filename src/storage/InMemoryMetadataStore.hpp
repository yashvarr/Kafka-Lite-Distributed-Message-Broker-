#pragma once

#include "storage/IMetadataStore.hpp"
#include <deque>
#include <map>
#include <mutex>

/**
 * @brief In-memory implementation of IMetadataStore for testing and development
 */
class InMemoryMetadataStore : public IMetadataStore {
public:
    InMemoryMetadataStore();

    // Read operations
    bool is_topic_known(const std::string &topic_name) const override;
    bool is_uuid_known(const std::vector<uint8_t> &uuid) const override;
    std::vector<uint8_t> get_topic_uuid(const std::string &topic_name) const override;
    std::vector<std::vector<uint8_t>> get_serialized_partitions(const std::vector<uint8_t> &topic_id) const override;
    std::vector<uint8_t> getEntireRecBatch(const std::vector<uint8_t> &uuid, const int32_t &parIndex) const override;

    std::vector<std::string> get_all_topic_names() const override;
    TopicInfo get_topic_info(const std::string &topic_name) const override;
    TopicInfo get_topic_info_by_uuid(const std::vector<uint8_t> &uuid) const override;
    PartitionInfo get_partition_info(const std::vector<uint8_t> &topic_id, int32_t partition_id) const override;

    // Write operations
    bool create_topic(const std::string &topic_name, int32_t num_partitions, int16_t replication_factor) override;
    bool create_partitions(const std::string &topic_name, int32_t new_partition_count) override;
    bool delete_topic(const std::string &topic_name) override;

    // Message operations
    int64_t append_message(const std::vector<uint8_t> &topic_id, int32_t partition_id,
                           const std::vector<uint8_t> &key, const std::vector<uint8_t> &value,
                           const std::vector<std::pair<std::string, std::vector<uint8_t>>> &headers) override;

    std::vector<MessageRecord> fetch_messages(const std::vector<uint8_t> &topic_id, int32_t partition_id,
                                              int64_t offset, int32_t max_bytes) const override;

    // Offset operations
    int64_t get_earliest_offset(const std::vector<uint8_t> &topic_id, int32_t partition_id) const override;
    int64_t get_latest_offset(const std::vector<uint8_t> &topic_id, int32_t partition_id) const override;
    int64_t get_offset_by_timestamp(const std::vector<uint8_t> &topic_id, int32_t partition_id, int64_t timestamp) const override;

private:
    mutable std::mutex mutex_;
    std::map<std::string, TopicInfo> topics_;
    std::map<std::vector<uint8_t>, std::string> uuid_to_name_;

    // Messages storage: topic_uuid -> partition_id -> messages
    std::map<std::vector<uint8_t>, std::map<int32_t, std::deque<MessageRecord>>> messages_;

    std::vector<uint8_t> generate_uuid();
    std::vector<uint8_t> serialize_partition(const PartitionInfo &partition) const;
    std::vector<uint8_t> create_record_batch(const std::vector<MessageRecord> &messages) const;
};