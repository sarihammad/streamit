#include "streamit/controller/topic_manager.h"
#include "streamit/common/status.h"
#include <algorithm>

namespace streamit::controller {

TopicManager::TopicManager() = default;

Result<void> TopicManager::CreateTopic(const std::string& name, int32_t partitions,
                                       int32_t replication_factor) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  if (topics_.find(name) != topics_.end()) {
    return Error<void>(absl::StatusCode::kAlreadyExists, "Topic already exists: " + name);
  }

  if (partitions <= 0) {
    return Error<void>(absl::StatusCode::kInvalidArgument, "Partitions must be positive");
  }

  if (replication_factor <= 0) {
    return Error<void>(absl::StatusCode::kInvalidArgument, "Replication factor must be positive");
  }

  TopicInfo topic_info(name, partitions, replication_factor);
  topic_info.partition_infos = GeneratePartitionAssignments(name, partitions, replication_factor);

  topics_[name] = std::move(topic_info);

  return Ok();
}

Result<TopicInfo> TopicManager::GetTopic(const std::string& name) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = topics_.find(name);
  if (it == topics_.end()) {
    return Error<TopicInfo>(absl::StatusCode::kNotFound, "Topic not found: " + name);
  }

  return Ok(it->second);
}

std::vector<std::string> TopicManager::ListTopics() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<std::string> topic_names;
  topic_names.reserve(topics_.size());

  for (const auto& topic_entry : topics_) {
    topic_names.push_back(topic_entry.first);
  }

  std::sort(topic_names.begin(), topic_names.end());
  return topic_names;
}

bool TopicManager::TopicExists(const std::string& name) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return topics_.find(name) != topics_.end();
}

Result<void> TopicManager::DeleteTopic(const std::string& name) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = topics_.find(name);
  if (it == topics_.end()) {
    return Error<void>(absl::StatusCode::kNotFound, "Topic not found: " + name);
  }

  topics_.erase(it);
  return Ok();
}

Result<void> TopicManager::UpdatePartitionLeader(const std::string& topic, int32_t partition, int32_t leader) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto topic_it = topics_.find(topic);
  if (topic_it == topics_.end()) {
    return Error<void>(absl::StatusCode::kNotFound, "Topic not found: " + topic);
  }

  auto& partition_infos = topic_it->second.partition_infos;
  auto partition_it = std::find_if(partition_infos.begin(), partition_infos.end(),
                                   [partition](const PartitionInfo& info) { return info.partition == partition; });

  if (partition_it == partition_infos.end()) {
    return Error<void>(absl::StatusCode::kNotFound, "Partition not found: " + topic + ":" + std::to_string(partition));
  }

  partition_it->leader = leader;
  return Ok();
}

Result<void> TopicManager::UpdatePartitionHighWaterMark(const std::string& topic, int32_t partition,
                                                        int64_t high_watermark) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto topic_it = topics_.find(topic);
  if (topic_it == topics_.end()) {
    return Error<void>(absl::StatusCode::kNotFound, "Topic not found: " + topic);
  }

  auto& partition_infos = topic_it->second.partition_infos;
  auto partition_it = std::find_if(partition_infos.begin(), partition_infos.end(),
                                   [partition](const PartitionInfo& info) { return info.partition == partition; });

  if (partition_it == partition_infos.end()) {
    return Error<void>(absl::StatusCode::kNotFound, "Partition not found: " + topic + ":" + std::to_string(partition));
  }

  partition_it->high_watermark = high_watermark;
  return Ok();
}

Result<PartitionInfo> TopicManager::GetPartitionInfo(const std::string& topic, int32_t partition) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto topic_it = topics_.find(topic);
  if (topic_it == topics_.end()) {
    return Error<PartitionInfo>(absl::StatusCode::kNotFound, "Topic not found: " + topic);
  }

  const auto& partition_infos = topic_it->second.partition_infos;
  auto partition_it = std::find_if(partition_infos.begin(), partition_infos.end(),
                                   [partition](const PartitionInfo& info) { return info.partition == partition; });

  if (partition_it == partition_infos.end()) {
    return Error<PartitionInfo>(absl::StatusCode::kNotFound,
                                "Partition not found: " + topic + ":" + std::to_string(partition));
  }

  return Ok(*partition_it);
}

std::vector<PartitionInfo> TopicManager::GeneratePartitionAssignments(const std::string& topic, int32_t partitions,
                                                                      int32_t replication_factor) const noexcept {

  std::vector<PartitionInfo> partition_infos;
  partition_infos.reserve(partitions);

  // Simplified assignment - in practice would use a more sophisticated algorithm
  for (int32_t i = 0; i < partitions; ++i) {
    std::vector<int32_t> replicas;
    replicas.reserve(replication_factor);

    // Assign replicas in round-robin fashion (simplified)
    for (int32_t j = 0; j < replication_factor; ++j) {
      replicas.push_back(j % 3); // Assuming 3 brokers for now
    }

    // First replica is the leader
    int32_t leader = replicas[0];

    partition_infos.emplace_back(i, leader, std::move(replicas), 0);
  }

  return partition_infos;
}

} // namespace streamit::controller
