#include "storage/InMemoryMetadataStore.hpp"
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <random>

InMemoryMetadataStore::InMemoryMetadataStore() {}

std::vector<uint8_t> InMemoryMetadataStore::generate_uuid() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    std::vector<uint8_t> uuid(16);
    uint64_t part1 = dis(gen);
    uint64_t part2 = dis(gen);

    std::memcpy(uuid.data(), &part1, 8);
    std::memcpy(uuid.data() + 8, &part2, 8);

    return uuid;
}

bool InMemoryMetadataStore::is_topic_known(const std::string &topic_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return topics_.find(topic_name) != topics_.end();
}

bool InMemoryMetadataStore::is_uuid_known(const std::vector<uint8_t> &uuid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return uuid_to_name_.find(uuid) != uuid_to_name_.end();
}

std::vector<uint8_t> InMemoryMetadataStore::get_topic_uuid(const std::string &topic_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = topics_.find(topic_name);
    if (it != topics_.end()) {
        return it->second.uuid;
    }
    return std::vector<uint8_t>(16, 0);
}

std::vector<std::string> InMemoryMetadataStore::get_all_topic_names() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> names;
    for (const auto &[name, _] : topics_) {
        names.push_back(name);
    }
    return names;
}

TopicInfo InMemoryMetadataStore::get_topic_info(const std::string &topic_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = topics_.find(topic_name);
    if (it != topics_.end()) {
        return it->second;
    }
    return TopicInfo{};
}

TopicInfo InMemoryMetadataStore::get_topic_info_by_uuid(const std::vector<uint8_t> &uuid) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = uuid_to_name_.find(uuid);
    if (it != uuid_to_name_.end()) {
        return topics_.at(it->second);
    }
    return TopicInfo{};
}

PartitionInfo InMemoryMetadataStore::get_partition_info(const std::vector<uint8_t> &topic_id, int32_t partition_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = uuid_to_name_.find(topic_id);
    if (it != uuid_to_name_.end()) {
        const auto &topic = topics_.at(it->second);
        auto pit = topic.partitions.find(partition_id);
        if (pit != topic.partitions.end()) {
            return pit->second;
        }
    }
    return PartitionInfo{};
}

std::vector<uint8_t> InMemoryMetadataStore::serialize_partition(const PartitionInfo &partition) const {
    std::vector<uint8_t> result;

    int16_t error_code = 0;
    result.insert(result.end(), reinterpret_cast<const uint8_t *>(&error_code),
                  reinterpret_cast<const uint8_t *>(&error_code) + sizeof(error_code));

    int32_t pid = htonl(partition.partition_id);
    result.insert(result.end(), reinterpret_cast<const uint8_t *>(&pid),
                  reinterpret_cast<const uint8_t *>(&pid) + sizeof(pid));

    int32_t leader = htonl(partition.leader);
    result.insert(result.end(), reinterpret_cast<const uint8_t *>(&leader),
                  reinterpret_cast<const uint8_t *>(&leader) + sizeof(leader));

    int32_t epoch = htonl(partition.leader_epoch);
    result.insert(result.end(), reinterpret_cast<const uint8_t *>(&epoch),
                  reinterpret_cast<const uint8_t *>(&epoch) + sizeof(epoch));

    result.push_back(static_cast<uint8_t>(partition.replicas.size() + 1));
    for (int32_t replica : partition.replicas) {
        int32_t r = htonl(replica);
        result.insert(result.end(), reinterpret_cast<const uint8_t *>(&r),
                      reinterpret_cast<const uint8_t *>(&r) + sizeof(r));
    }

    result.push_back(static_cast<uint8_t>(partition.isr.size() + 1));
    for (int32_t isr : partition.isr) {
        int32_t i = htonl(isr);
        result.insert(result.end(), reinterpret_cast<const uint8_t *>(&i),
                      reinterpret_cast<const uint8_t *>(&i) + sizeof(i));
    }

    result.push_back(1); // eligible_leader_replicas (empty)
    result.push_back(1); // last_known_elr (empty)
    result.push_back(1); // offline_replicas (empty)
    result.push_back(0); // tag buffer

    return result;
}

std::vector<std::vector<uint8_t>> InMemoryMetadataStore::get_serialized_partitions(const std::vector<uint8_t> &topic_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::vector<uint8_t>> result;

    auto it = uuid_to_name_.find(topic_id);
    if (it != uuid_to_name_.end()) {
        const auto &topic = topics_.at(it->second);
        for (const auto &[_, partition] : topic.partitions) {
            result.push_back(serialize_partition(partition));
        }
    }

    return result;
}

bool InMemoryMetadataStore::create_topic(const std::string &topic_name, int32_t num_partitions, int16_t replication_factor) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (topics_.find(topic_name) != topics_.end()) {
        return false; // Topic already exists
    }

    TopicInfo topic;
    topic.name = topic_name;
    topic.uuid = generate_uuid();
    topic.is_internal = false;

    for (int32_t i = 0; i < num_partitions; ++i) {
        PartitionInfo partition;
        partition.partition_id = i;
        partition.leader = 0;
        partition.leader_epoch = 0;
        partition.replicas = {0};
        partition.isr = {0};
        partition.log_start_offset = 0;
        partition.high_watermark = 0;

        topic.partitions[i] = partition;
    }

    topics_[topic_name] = topic;
    uuid_to_name_[topic.uuid] = topic_name;

    return true;
}

bool InMemoryMetadataStore::create_partitions(const std::string &topic_name, int32_t new_partition_count) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = topics_.find(topic_name);
    if (it == topics_.end()) {
        return false; // Topic doesn't exist
    }

    auto &topic = it->second;
    int32_t current_count = topic.partitions.size();

    if (new_partition_count <= current_count) {
        return false; // Cannot decrease partition count
    }

    for (int32_t i = current_count; i < new_partition_count; ++i) {
        PartitionInfo partition;
        partition.partition_id = i;
        partition.leader = 0;
        partition.leader_epoch = 0;
        partition.replicas = {0};
        partition.isr = {0};
        partition.log_start_offset = 0;
        partition.high_watermark = 0;

        topic.partitions[i] = partition;
    }

    return true;
}

bool InMemoryMetadataStore::delete_topic(const std::string &topic_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = topics_.find(topic_name);
    if (it == topics_.end()) {
        return false;
    }

    uuid_to_name_.erase(it->second.uuid);
    messages_.erase(it->second.uuid);
    topics_.erase(it);

    return true;
}

int64_t InMemoryMetadataStore::append_message(const std::vector<uint8_t> &topic_id, int32_t partition_id,
                                              const std::vector<uint8_t> &key, const std::vector<uint8_t> &value,
                                              const std::vector<std::pair<std::string, std::vector<uint8_t>>> &headers) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto &partition_messages = messages_[topic_id][partition_id];

    int64_t offset = partition_messages.empty() ? 0 : partition_messages.back().offset + 1;
    int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();

    MessageRecord record;
    record.offset = offset;
    record.timestamp = timestamp;
    record.key = key;
    record.value = value;
    record.headers = headers;

    partition_messages.push_back(record);

    // Update high watermark
    auto topic_it = uuid_to_name_.find(topic_id);
    if (topic_it != uuid_to_name_.end()) {
        auto &topic = topics_[topic_it->second];
        auto part_it = topic.partitions.find(partition_id);
        if (part_it != topic.partitions.end()) {
            part_it->second.high_watermark = offset + 1;
        }
    }

    return offset;
}

std::vector<MessageRecord> InMemoryMetadataStore::fetch_messages(const std::vector<uint8_t> &topic_id, int32_t partition_id,
                                                                 int64_t offset, int32_t max_bytes) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<MessageRecord> result;

    auto topic_it = messages_.find(topic_id);
    if (topic_it == messages_.end()) {
        return result;
    }

    auto partition_it = topic_it->second.find(partition_id);
    if (partition_it == topic_it->second.end()) {
        return result;
    }

    const auto &messages = partition_it->second;
    int32_t bytes_accumulated = 0;

    for (const auto &msg : messages) {
        if (msg.offset >= offset) {
            int32_t msg_size = msg.key.size() + msg.value.size() + 100; // Approximate size
            if (bytes_accumulated + msg_size > max_bytes && !result.empty()) {
                break;
            }
            result.push_back(msg);
            bytes_accumulated += msg_size;
        }
    }

    return result;
}

int64_t InMemoryMetadataStore::get_earliest_offset(const std::vector<uint8_t> &topic_id, int32_t partition_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto topic_it = messages_.find(topic_id);
    if (topic_it == messages_.end()) {
        return 0;
    }

    auto partition_it = topic_it->second.find(partition_id);
    if (partition_it == topic_it->second.end() || partition_it->second.empty()) {
        return 0;
    }

    return partition_it->second.front().offset;
}

int64_t InMemoryMetadataStore::get_latest_offset(const std::vector<uint8_t> &topic_id, int32_t partition_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto topic_it = messages_.find(topic_id);
    if (topic_it == messages_.end()) {
        return 0;
    }

    auto partition_it = topic_it->second.find(partition_id);
    if (partition_it == topic_it->second.end() || partition_it->second.empty()) {
        return 0;
    }

    return partition_it->second.back().offset + 1;
}

int64_t InMemoryMetadataStore::get_offset_by_timestamp(const std::vector<uint8_t> &topic_id, int32_t partition_id, int64_t timestamp) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto topic_it = messages_.find(topic_id);
    if (topic_it == messages_.end()) {
        return -1;
    }

    auto partition_it = topic_it->second.find(partition_id);
    if (partition_it == topic_it->second.end()) {
        return -1;
    }

    const auto &messages = partition_it->second;
    for (const auto &msg : messages) {
        if (msg.timestamp >= timestamp) {
            return msg.offset;
        }
    }

    return messages.empty() ? 0 : messages.back().offset + 1;
}

std::vector<uint8_t> InMemoryMetadataStore::create_record_batch(const std::vector<MessageRecord> &messages) const {
    // Simplified record batch creation
    std::vector<uint8_t> batch;

    // This is a placeholder - real implementation would follow Kafka's record batch format
    for (const auto &msg : messages) {
        // Add message data
        batch.insert(batch.end(), msg.value.begin(), msg.value.end());
    }

    return batch;
}

std::vector<uint8_t> InMemoryMetadataStore::getEntireRecBatch(const std::vector<uint8_t> &uuid, const int32_t &parIndex) const {
    auto messages = fetch_messages(uuid, parIndex, 0, 1024 * 1024); // Fetch up to 1MB
    return create_record_batch(messages);
}