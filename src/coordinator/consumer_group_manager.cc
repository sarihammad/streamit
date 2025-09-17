#include "streamit/coordinator/consumer_group_manager.h"
#include "streamit/common/status.h"
#include <algorithm>
#include <random>

namespace streamit::coordinator {

ConsumerGroupManager::ConsumerGroupManager(int32_t heartbeat_interval_ms, int32_t session_timeout_ms)
    : heartbeat_interval_ms_(heartbeat_interval_ms), session_timeout_ms_(session_timeout_ms) {
}

Result<void> ConsumerGroupManager::JoinGroup(const std::string& group_id, const std::string& member_id,
                                             const std::vector<std::string>& topics) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  // Find or create group
  auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    groups_[group_id] = ConsumerGroup(group_id);
    group_it = groups_.find(group_id);
  }

  ConsumerGroup& group = group_it->second;

  // Check if member already exists
  auto member_it = std::find_if(group.members.begin(), group.members.end(),
                                [&member_id](const ConsumerMember& member) { return member.member_id == member_id; });

  if (member_it != group.members.end()) {
    // Update existing member
    member_it->topics = topics;
    member_it->last_heartbeat = std::chrono::steady_clock::now();
    member_it->active = true;
  } else {
    // Add new member
    group.members.emplace_back(member_id, topics);
  }

  // Check if rebalancing is needed
  if (NeedsRebalancing(group_id)) {
    auto rebalance_result = RebalanceGroup(group_id);
    if (!rebalance_result.ok()) {
      return rebalance_result;
    }
  }

  return Ok();
}

Result<void> ConsumerGroupManager::LeaveGroup(const std::string& group_id, const std::string& member_id) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    return Error<void>(absl::StatusCode::kNotFound, "Group not found: " + group_id);
  }

  ConsumerGroup& group = group_it->second;

  // Remove member
  group.members.erase(
      std::remove_if(group.members.begin(), group.members.end(),
                     [&member_id](const ConsumerMember& member) { return member.member_id == member_id; }),
      group.members.end());

  // Rebalance if needed
  if (NeedsRebalancing(group_id)) {
    auto rebalance_result = RebalanceGroup(group_id);
    if (!rebalance_result.ok()) {
      return rebalance_result;
    }
  }

  return Ok();
}

Result<void> ConsumerGroupManager::Heartbeat(const std::string& group_id, const std::string& member_id) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    return Error<void>(absl::StatusCode::kNotFound, "Group not found: " + group_id);
  }

  ConsumerGroup& group = group_it->second;

  // Find member
  auto member_it = std::find_if(group.members.begin(), group.members.end(),
                                [&member_id](const ConsumerMember& member) { return member.member_id == member_id; });

  if (member_it == group.members.end()) {
    return Error<void>(absl::StatusCode::kNotFound, "Member not found: " + member_id);
  }

  // Update heartbeat
  member_it->last_heartbeat = std::chrono::steady_clock::now();
  member_it->active = true;

  return Ok();
}

Result<std::vector<PartitionAssignment>> ConsumerGroupManager::GetAssignments(
    const std::string& group_id, const std::string& member_id) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    return Error<std::vector<PartitionAssignment>>(absl::StatusCode::kNotFound, "Group not found: " + group_id);
  }

  const ConsumerGroup& group = group_it->second;

  // Find member assignments
  auto assignment_it = group.assignments.find(member_id);
  if (assignment_it == group.assignments.end()) {
    return Ok(std::vector<PartitionAssignment>{});
  }

  return Ok(assignment_it->second);
}

Result<void> ConsumerGroupManager::CommitOffset(const std::string& group_id, const std::string& topic,
                                                int32_t partition, int64_t offset) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    return Error<void>(absl::StatusCode::kNotFound, "Group not found: " + group_id);
  }

  ConsumerGroup& group = group_it->second;
  group.committed_offsets[topic][partition] = offset;

  return Ok();
}

Result<int64_t> ConsumerGroupManager::GetCommittedOffset(const std::string& group_id, const std::string& topic,
                                                         int32_t partition) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    return Error<int64_t>(absl::StatusCode::kNotFound, "Group not found: " + group_id);
  }

  const ConsumerGroup& group = group_it->second;

  auto topic_it = group.committed_offsets.find(topic);
  if (topic_it == group.committed_offsets.end()) {
    return Ok(0); // Default to beginning
  }

  auto partition_it = topic_it->second.find(partition);
  if (partition_it == topic_it->second.end()) {
    return Ok(0); // Default to beginning
  }

  return Ok(partition_it->second);
}

bool ConsumerGroupManager::NeedsRebalancing(const std::string& group_id) const noexcept {
  auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    return false;
  }

  const ConsumerGroup& group = group_it->second;

  // Check if any member is inactive
  for (const auto& member : group.members) {
    if (!IsMemberActive(member)) {
      return true;
    }
  }

  // Check if group is empty
  if (group.members.empty()) {
    return false;
  }

  // Check if assignments are missing for any member
  for (const auto& member : group.members) {
    if (group.assignments.find(member.member_id) == group.assignments.end()) {
      return true;
    }
  }

  return false;
}

Result<void> ConsumerGroupManager::RebalanceGroup(const std::string& group_id) noexcept {
  auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    return Error<void>(absl::StatusCode::kNotFound, "Group not found: " + group_id);
  }

  ConsumerGroup& group = group_it->second;

  // Remove inactive members
  group.members.erase(std::remove_if(group.members.begin(), group.members.end(),
                                     [this](const ConsumerMember& member) { return !IsMemberActive(member); }),
                      group.members.end());

  if (group.members.empty()) {
    group.assignments.clear();
    return Ok();
  }

  // Assign partitions
  group.assignments = AssignPartitions(group);
  group.last_rebalance = std::chrono::steady_clock::now();

  return Ok();
}

void ConsumerGroupManager::CleanupInactiveMembers() noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  for (auto& group_pair : groups_) {
    ConsumerGroup& group = group_pair.second;

    // Remove inactive members
    group.members.erase(std::remove_if(group.members.begin(), group.members.end(),
                                       [this](const ConsumerMember& member) { return !IsMemberActive(member); }),
                        group.members.end());

    // Rebalance if needed
    if (NeedsRebalancing(group_pair.first)) {
      RebalanceGroup(group_pair.first);
    }
  }
}

std::vector<std::string> ConsumerGroupManager::ListGroups() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<std::string> group_ids;
  group_ids.reserve(groups_.size());

  for (const auto& group_pair : groups_) {
    group_ids.push_back(group_pair.first);
  }

  std::sort(group_ids.begin(), group_ids.end());
  return group_ids;
}

Result<ConsumerGroup> ConsumerGroupManager::GetGroup(const std::string& group_id) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    return Error<ConsumerGroup>(absl::StatusCode::kNotFound, "Group not found: " + group_id);
  }

  return Ok(group_it->second);
}

std::unordered_map<std::string, std::vector<PartitionAssignment>> ConsumerGroupManager::AssignPartitions(
    const ConsumerGroup& group) const noexcept {
  std::unordered_map<std::string, std::vector<PartitionAssignment>> assignments;

  if (group.members.empty()) {
    return assignments;
  }

  // Get all topics for the group
  auto topics = GetGroupTopics(group);

  // Simple round-robin assignment
  size_t member_index = 0;
  for (const auto& topic : topics) {
    // For simplicity, assume 6 partitions per topic
    std::vector<int32_t> partitions = {0, 1, 2, 3, 4, 5};

    for (int32_t partition : partitions) {
      const auto& member = group.members[member_index % group.members.size()];
      assignments[member.member_id].emplace_back(topic, std::vector<int32_t>{partition});
      member_index++;
    }
  }

  return assignments;
}

bool ConsumerGroupManager::IsMemberActive(const ConsumerMember& member) const noexcept {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - member.last_heartbeat).count();
  return member.active && elapsed < session_timeout_ms_;
}

std::unordered_set<std::string> ConsumerGroupManager::GetGroupTopics(const ConsumerGroup& group) const noexcept {
  std::unordered_set<std::string> topics;

  for (const auto& member : group.members) {
    for (const auto& topic : member.topics) {
      topics.insert(topic);
    }
  }

  return topics;
}

} // namespace streamit::coordinator
