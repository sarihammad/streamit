#include <gtest/gtest.h>
#include "streamit/coordinator/consumer_group_manager.h"
#include <string>
#include <vector>
#include <chrono>
#include <thread>

namespace streamit::coordinator {
namespace {

TEST(ConsumerGroupManagerTest, JoinGroup) {
  ConsumerGroupManager manager(10000, 30000);
  
  std::vector<std::string> topics = {"topic1", "topic2"};
  auto result = manager.JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(result.ok());
  
  auto groups = manager.ListGroups();
  EXPECT_EQ(groups.size(), 1);
  EXPECT_EQ(groups[0], "group1");
}

TEST(ConsumerGroupManagerTest, LeaveGroup) {
  ConsumerGroupManager manager(10000, 30000);
  
  std::vector<std::string> topics = {"topic1"};
  auto join_result = manager.JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  auto leave_result = manager.LeaveGroup("group1", "member1");
  EXPECT_TRUE(leave_result.ok());
  
  auto groups = manager.ListGroups();
  EXPECT_EQ(groups.size(), 1); // Group still exists but no members
}

TEST(ConsumerGroupManagerTest, Heartbeat) {
  ConsumerGroupManager manager(10000, 30000);
  
  std::vector<std::string> topics = {"topic1"};
  auto join_result = manager.JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  auto heartbeat_result = manager.Heartbeat("group1", "member1");
  EXPECT_TRUE(heartbeat_result.ok());
}

TEST(ConsumerGroupManagerTest, GetAssignments) {
  ConsumerGroupManager manager(10000, 30000);
  
  std::vector<std::string> topics = {"topic1"};
  auto join_result = manager.JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  auto assignments_result = manager.GetAssignments("group1", "member1");
  EXPECT_TRUE(assignments_result.ok());
  
  const auto& assignments = assignments_result.value();
  EXPECT_FALSE(assignments.empty());
}

TEST(ConsumerGroupManagerTest, CommitOffset) {
  ConsumerGroupManager manager(10000, 30000);
  
  auto commit_result = manager.CommitOffset("group1", "topic1", 0, 1000);
  EXPECT_TRUE(commit_result.ok());
  
  auto offset_result = manager.GetCommittedOffset("group1", "topic1", 0);
  EXPECT_TRUE(offset_result.ok());
  EXPECT_EQ(offset_result.value(), 1000);
}

TEST(ConsumerGroupManagerTest, GetCommittedOffset) {
  ConsumerGroupManager manager(10000, 30000);
  
  // Get offset for non-existent group/topic/partition should return 0
  auto offset_result = manager.GetCommittedOffset("group1", "topic1", 0);
  EXPECT_TRUE(offset_result.ok());
  EXPECT_EQ(offset_result.value(), 0);
}

TEST(ConsumerGroupManagerTest, NeedsRebalancing) {
  ConsumerGroupManager manager(10000, 30000);
  
  std::vector<std::string> topics = {"topic1"};
  auto join_result = manager.JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  // Group with members should need rebalancing initially
  EXPECT_TRUE(manager.NeedsRebalancing("group1"));
  
  // Rebalance the group
  auto rebalance_result = manager.RebalanceGroup("group1");
  EXPECT_TRUE(rebalance_result.ok());
  
  // After rebalancing, should not need rebalancing
  EXPECT_FALSE(manager.NeedsRebalancing("group1"));
}

TEST(ConsumerGroupManagerTest, RebalanceGroup) {
  ConsumerGroupManager manager(10000, 30000);
  
  std::vector<std::string> topics = {"topic1"};
  auto join_result = manager.JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  auto rebalance_result = manager.RebalanceGroup("group1");
  EXPECT_TRUE(rebalance_result.ok());
  
  // Should have assignments after rebalancing
  auto assignments_result = manager.GetAssignments("group1", "member1");
  EXPECT_TRUE(assignments_result.ok());
  EXPECT_FALSE(assignments_result.value().empty());
}

TEST(ConsumerGroupManagerTest, CleanupInactiveMembers) {
  ConsumerGroupManager manager(1000, 2000); // Short timeouts for testing
  
  std::vector<std::string> topics = {"topic1"};
  auto join_result = manager.JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  // Wait for member to become inactive
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  
  // Cleanup should remove inactive members
  manager.CleanupInactiveMembers();
  
  // Group should still exist but with no active members
  auto groups = manager.ListGroups();
  EXPECT_EQ(groups.size(), 1);
}

TEST(ConsumerGroupManagerTest, GetGroup) {
  ConsumerGroupManager manager(10000, 30000);
  
  std::vector<std::string> topics = {"topic1"};
  auto join_result = manager.JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  auto group_result = manager.GetGroup("group1");
  EXPECT_TRUE(group_result.ok());
  
  const auto& group = group_result.value();
  EXPECT_EQ(group.group_id, "group1");
  EXPECT_EQ(group.members.size(), 1);
  EXPECT_EQ(group.members[0].member_id, "member1");
}

TEST(ConsumerGroupManagerTest, GetNonExistentGroup) {
  ConsumerGroupManager manager(10000, 30000);
  
  auto group_result = manager.GetGroup("non-existent-group");
  EXPECT_FALSE(group_result.ok());
  EXPECT_EQ(group_result.status().code(), absl::StatusCode::kNotFound);
}

}  
}

