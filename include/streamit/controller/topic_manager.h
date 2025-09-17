#pragma once

#include "streamit/common/result.h"
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace streamit::controller {

// Partition information
struct PartitionInfo {
  int32_t partition;
  int32_t leader;
  std::vector<int32_t> replicas;
  int64_t high_watermark;

  PartitionInfo() = default;
  PartitionInfo(int32_t partition, int32_t leader, std::vector<int32_t> replicas, int64_t hwm)
      : partition(partition), leader(leader), replicas(std::move(replicas)), high_watermark(hwm) {
  }
};

// Topic information
struct TopicInfo {
  std::string name;
  int32_t partitions;
  int32_t replication_factor;
  std::vector<PartitionInfo> partition_infos;

  TopicInfo() = default;
  TopicInfo(std::string name, int32_t partitions, int32_t replication_factor)
      : name(std::move(name)), partitions(partitions), replication_factor(replication_factor) {
  }
};

// Topic manager for handling topic metadata
class TopicManager {
public:
  // Constructor
  TopicManager();

  // Create a new topic
  [[nodiscard]] Result<void> CreateTopic(const std::string& name, int32_t partitions,
                                         int32_t replication_factor) noexcept;

  // Get topic information
  [[nodiscard]] Result<TopicInfo> GetTopic(const std::string& name) const noexcept;

  // List all topics
  [[nodiscard]] std::vector<std::string> ListTopics() const noexcept;

  // Check if topic exists
  [[nodiscard]] bool TopicExists(const std::string& name) const noexcept;

  // Delete a topic
  [[nodiscard]] Result<void> DeleteTopic(const std::string& name) noexcept;

  // Update partition leader
  [[nodiscard]] Result<void> UpdatePartitionLeader(const std::string& topic, int32_t partition,
                                                   int32_t leader) noexcept;

  // Update partition high water mark
  [[nodiscard]] Result<void> UpdatePartitionHighWaterMark(const std::string& topic, int32_t partition,
                                                          int64_t high_watermark) noexcept;

  // Get partition information
  [[nodiscard]] Result<PartitionInfo> GetPartitionInfo(const std::string& topic, int32_t partition) const noexcept;

private:
  // Topic name -> TopicInfo
  std::unordered_map<std::string, TopicInfo> topics_;
  mutable std::mutex mutex_;

  // Helper to generate partition assignments
  [[nodiscard]] std::vector<PartitionInfo> GeneratePartitionAssignments(const std::string& topic, int32_t partitions,
                                                                        int32_t replication_factor) const noexcept;
};

} // namespace streamit::controller
