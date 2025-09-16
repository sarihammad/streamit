#pragma once

#include "streamit/common/result.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <chrono>
#include <cstdint>

namespace streamit::coordinator {

// Consumer group member
struct ConsumerMember {
  std::string member_id;
  std::vector<std::string> topics;
  std::chrono::steady_clock::time_point last_heartbeat;
  bool active;
  
  ConsumerMember() = default;
  ConsumerMember(std::string member_id, std::vector<std::string> topics)
    : member_id(std::move(member_id)), topics(std::move(topics)), 
      last_heartbeat(std::chrono::steady_clock::now()), active(true) {}
};

// Partition assignment
struct PartitionAssignment {
  std::string topic;
  std::vector<int32_t> partitions;
  
  PartitionAssignment() = default;
  PartitionAssignment(std::string topic, std::vector<int32_t> partitions)
    : topic(std::move(topic)), partitions(std::move(partitions)) {}
};

// Consumer group
struct ConsumerGroup {
  std::string group_id;
  std::vector<ConsumerMember> members;
  std::unordered_map<std::string, std::vector<PartitionAssignment>> assignments;
  std::unordered_map<std::string, std::unordered_map<int32_t, int64_t>> committed_offsets;
  std::chrono::steady_clock::time_point last_rebalance;
  
  ConsumerGroup() = default;
  ConsumerGroup(std::string group_id) : group_id(std::move(group_id)) {}
};

// Consumer group manager
class ConsumerGroupManager {
public:
  // Constructor
  ConsumerGroupManager(int32_t heartbeat_interval_ms, int32_t session_timeout_ms);
  
  // Join a consumer group
  [[nodiscard]] Result<void> JoinGroup(const std::string& group_id, 
                                      const std::string& member_id,
                                      const std::vector<std::string>& topics) noexcept;
  
  // Leave a consumer group
  [[nodiscard]] Result<void> LeaveGroup(const std::string& group_id, 
                                       const std::string& member_id) noexcept;
  
  // Heartbeat from a consumer
  [[nodiscard]] Result<void> Heartbeat(const std::string& group_id, 
                                      const std::string& member_id) noexcept;
  
  // Get partition assignments for a consumer
  [[nodiscard]] Result<std::vector<PartitionAssignment>> GetAssignments(
    const std::string& group_id, const std::string& member_id) const noexcept;
  
  // Commit offset for a partition
  [[nodiscard]] Result<void> CommitOffset(const std::string& group_id,
                                         const std::string& topic,
                                         int32_t partition,
                                         int64_t offset) noexcept;
  
  // Get committed offset for a partition
  [[nodiscard]] Result<int64_t> GetCommittedOffset(const std::string& group_id,
                                                  const std::string& topic,
                                                  int32_t partition) const noexcept;
  
  // Check if a group needs rebalancing
  [[nodiscard]] bool NeedsRebalancing(const std::string& group_id) const noexcept;
  
  // Perform rebalancing for a group
  [[nodiscard]] Result<void> RebalanceGroup(const std::string& group_id) noexcept;
  
  // Clean up inactive members
  [[nodiscard]] void CleanupInactiveMembers() noexcept;
  
  // Get all consumer groups
  [[nodiscard]] std::vector<std::string> ListGroups() const noexcept;
  
  // Get group information
  [[nodiscard]] Result<ConsumerGroup> GetGroup(const std::string& group_id) const noexcept;

private:
  int32_t heartbeat_interval_ms_;
  int32_t session_timeout_ms_;
  
  // Group ID -> ConsumerGroup
  std::unordered_map<std::string, ConsumerGroup> groups_;
  mutable std::mutex mutex_;
  
  // Helper to assign partitions to members
  [[nodiscard]] std::unordered_map<std::string, std::vector<PartitionAssignment>> 
    AssignPartitions(const ConsumerGroup& group) const noexcept;
  
  // Helper to check if a member is active
  [[nodiscard]] bool IsMemberActive(const ConsumerMember& member) const noexcept;
  
  // Helper to get all topics for a group
  [[nodiscard]] std::unordered_set<std::string> GetGroupTopics(
    const ConsumerGroup& group) const noexcept;
};

}

