#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

/**
 * @brief Structure to hold partition information
 */
struct PartitionInfo {
    int32_t partition_id;
    int32_t leader;
    int32_t leader_epoch;
    std::vector<int32_t> replicas;
    std::vector<int32_t> isr;
    int64_t log_start_offset;
    int64_t high_watermark;
};

/**
 * @brief Structure to hold topic information
 */
struct TopicInfo {
    std::string name;
    std::vector<uint8_t> uuid;
    std::map<int32_t, PartitionInfo> partitions;
    bool is_internal;
};

/**
 * @brief Structure to hold a message record
 */
struct MessageRecord {
    int64_t offset;
    int64_t timestamp;
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
    std::vector<std::pair<std::string, std::vector<uint8_t>>> headers;
};

/**
 * @brief An interface for a data store that provides Kafka topic and partition metadata.
 */
class IMetadataStore {
public:
    virtual ~IMetadataStore() = default;

    // Read operations
    virtual bool is_topic_known(const std::string &topic_name) const = 0;
    virtual bool is_uuid_known(const std::vector<uint8_t> &uuid) const = 0;
    virtual std::vector<uint8_t> get_topic_uuid(const std::string &topic_name) const = 0;
    virtual std::vector<std::vector<uint8_t>> get_serialized_partitions(const std::vector<uint8_t> &topic_id) const = 0;
    virtual std::vector<uint8_t> getEntireRecBatch(const std::vector<uint8_t> &uuid, const int32_t &parIndex) const = 0;
    virtual std::vector<std::string> get_all_topic_names() const = 0;
    virtual TopicInfo get_topic_info(const std::string &topic_name) const = 0;
    virtual TopicInfo get_topic_info_by_uuid(const std::vector<uint8_t> &uuid) const = 0;
    virtual PartitionInfo get_partition_info(const std::vector<uint8_t> &topic_id, int32_t partition_id) const = 0;

    // Write operations
    virtual bool create_topic(const std::string &topic_name, int32_t num_partitions, int16_t replication_factor) = 0;
    virtual bool create_partitions(const std::string &topic_name, int32_t new_partition_count) = 0;
    virtual bool delete_topic(const std::string &topic_name) = 0;

    // Message operations
    virtual int64_t append_message(const std::vector<uint8_t> &topic_id, int32_t partition_id,
                                   const std::vector<uint8_t> &key, const std::vector<uint8_t> &value,
                                   const std::vector<std::pair<std::string, std::vector<uint8_t>>> &headers) = 0;

    virtual std::vector<MessageRecord> fetch_messages(const std::vector<uint8_t> &topic_id, int32_t partition_id,
                                                      int64_t offset, int32_t max_bytes) const = 0;

    // Offset operations
    virtual int64_t get_earliest_offset(const std::vector<uint8_t> &topic_id, int32_t partition_id) const = 0;
    virtual int64_t get_latest_offset(const std::vector<uint8_t> &topic_id, int32_t partition_id) const = 0;
    virtual int64_t get_offset_by_timestamp(const std::vector<uint8_t> &topic_id, int32_t partition_id, int64_t timestamp) const = 0;
};