#pragma once

#include "storage/IMetadataStore.hpp"
#include <vector>
#include <cstdint>
#include <set>
#include <map>
#include <string>

/**
 * @brief An implementation of IMetadataStore that loads and parses state from a
 * Kafka KRaft __cluster_metadata log file.
 */
class KRaftMetadataStore : public IMetadataStore
{
public:
    explicit KRaftMetadataStore(const std::string &log_path);

    // IMetadataStore Interface Implementation
    bool is_topic_known(const std::string &name) const override;
    bool is_uuid_known(const std::vector<uint8_t> &uuid) const override;
    std::vector<uint8_t> get_topic_uuid(const std::string &topicN) const override;
    std::vector<std::vector<uint8_t>> get_serialized_partitions(const std::vector<uint8_t> &topic_id) const override;
    std::vector<uint8_t> getEntireRecBatch(const std::vector<uint8_t> &uuid, const int32_t &parIndex) const override;

private:
    // State Variables
    std::vector<uint8_t> cluster_metadata;
    std::map<int64_t, int32_t> batch_info;
    std::vector<int32_t> records_num;
    std::set<std::string> topic_name;
    std::map<std::string, std::vector<uint8_t>> nameToTopicId;
    std::map<std::vector<uint8_t>, std::vector<std::vector<uint8_t>>> topicToPars;
    std::vector<std::vector<uint8_t>> partitions;
    std::size_t offsetToRec = 0;

    // Private Helper Methods
    int64_t toBigEndian(int64_t littleEndianVal);
    int64_t toLittleEndian(int64_t bigEndianVal);
    std::pair<int32_t, std::size_t> readZigZagVarint(std::size_t offset);
    std::pair<uint32_t, std::size_t> readUnsignedVarint(std::size_t offset);

    void getBatch_info();
    void toRecords();
    void getRecords_num();
    std::size_t getRecToValue(std::size_t offset);
    std::size_t toNextBatch(std::size_t offset);

    void parseTopicHelper(std::size_t offset);
    void parseTopic();

    void topicMatchPars(std::vector<uint8_t> uuid, std::vector<uint8_t> par);

    std::vector<uint8_t> parseParsHelper(std::size_t offset);
    void parsePars();
};