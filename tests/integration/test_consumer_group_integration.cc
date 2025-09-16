#include <gtest/gtest.h>
#include "streamit/coordinator/consumer_group_manager.h"
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <set>

namespace streamit::integration {
namespace {

class ConsumerGroupIntegrationTest : public ::testing::Test {
protected:
  void SetUp() override {
    group_manager_ = std::make_unique<streamit::coordinator::ConsumerGroupManager>(
      10000, 30000);
  }
  
  std::unique_ptr<streamit::coordinator::ConsumerGroupManager> group_manager_;
};

TEST_F(ConsumerGroupIntegrationTest, MultipleConsumers) {
  // Add multiple consumers to the same group
  std::vector<std::string> topics = {"topic1", "topic2"};
  
  auto join1_result = group_manager_->JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join1_result.ok());
  
  auto join2_result = group_manager_->JoinGroup("group1", "member2", topics);
  EXPECT_TRUE(join2_result.ok());
  
  // Both members should be in the group
  auto group_result = group_manager_->GetGroup("group1");
  EXPECT_TRUE(group_result.ok());
  EXPECT_EQ(group_result.value().members.size(), 2);
  
  // Both members should have assignments
  auto assignments1_result = group_manager_->GetAssignments("group1", "member1");
  EXPECT_TRUE(assignments1_result.ok());
  
  auto assignments2_result = group_manager_->GetAssignments("group1", "member2");
  EXPECT_TRUE(assignments2_result.ok());
  
  // Assignments should be different
  const auto& assignments1 = assignments1_result.value();
  const auto& assignments2 = assignments2_result.value();
  EXPECT_NE(assignments1, assignments2);
}

TEST_F(ConsumerGroupIntegrationTest, ConsumerLeaveAndRejoin) {
  std::vector<std::string> topics = {"topic1"};
  
  // Join group
  auto join_result = group_manager_->JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  // Leave group
  auto leave_result = group_manager_->LeaveGroup("group1", "member1");
  EXPECT_TRUE(leave_result.ok());
  
  // Rejoin group
  auto rejoin_result = group_manager_->JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(rejoin_result.ok());
  
  // Should have assignments after rejoin
  auto assignments_result = group_manager_->GetAssignments("group1", "member1");
  EXPECT_TRUE(assignments_result.ok());
}

TEST_F(ConsumerGroupIntegrationTest, OffsetCommitAndRetrieve) {
  std::vector<std::string> topics = {"topic1"};
  
  // Join group
  auto join_result = group_manager_->JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  // Commit offset
  auto commit_result = group_manager_->CommitOffset("group1", "topic1", 0, 1000);
  EXPECT_TRUE(commit_result.ok());
  
  // Retrieve offset
  auto offset_result = group_manager_->GetCommittedOffset("group1", "topic1", 0);
  EXPECT_TRUE(offset_result.ok());
  EXPECT_EQ(offset_result.value(), 1000);
  
  // Update offset
  auto update_result = group_manager_->CommitOffset("group1", "topic1", 0, 2000);
  EXPECT_TRUE(update_result.ok());
  
  // Verify updated offset
  auto updated_offset_result = group_manager_->GetCommittedOffset("group1", "topic1", 0);
  EXPECT_TRUE(updated_offset_result.ok());
  EXPECT_EQ(updated_offset_result.value(), 2000);
}

TEST_F(ConsumerGroupIntegrationTest, MultipleTopics) {
  std::vector<std::string> topics = {"topic1", "topic2", "topic3"};
  
  // Join group with multiple topics
  auto join_result = group_manager_->JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  // Get assignments
  auto assignments_result = group_manager_->GetAssignments("group1", "member1");
  EXPECT_TRUE(assignments_result.ok());
  
  const auto& assignments = assignments_result.value();
  
  // Should have assignments for all topics
  std::set<std::string> assigned_topics;
  for (const auto& assignment : assignments) {
    assigned_topics.insert(assignment.topic);
  }
  
  EXPECT_TRUE(assigned_topics.count("topic1"));
  EXPECT_TRUE(assigned_topics.count("topic2"));
  EXPECT_TRUE(assigned_topics.count("topic3"));
}

TEST_F(ConsumerGroupIntegrationTest, HeartbeatTimeout) {
  // Create manager with short timeouts
  auto short_timeout_manager = std::make_unique<streamit::coordinator::ConsumerGroupManager>(
    1000, 2000); // 1s heartbeat, 2s session timeout
  
  std::vector<std::string> topics = {"topic1"};
  
  // Join group
  auto join_result = short_timeout_manager->JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join_result.ok());
  
  // Wait for member to become inactive
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  
  // Cleanup inactive members
  short_timeout_manager->CleanupInactiveMembers();
  
  // Member should be removed
  auto group_result = short_timeout_manager->GetGroup("group1");
  EXPECT_TRUE(group_result.ok());
  EXPECT_EQ(group_result.value().members.size(), 0);
}

TEST_F(ConsumerGroupIntegrationTest, Rebalancing) {
  std::vector<std::string> topics = {"topic1"};
  
  // Add first member
  auto join1_result = group_manager_->JoinGroup("group1", "member1", topics);
  EXPECT_TRUE(join1_result.ok());
  
  // Rebalance group
  auto rebalance1_result = group_manager_->RebalanceGroup("group1");
  EXPECT_TRUE(rebalance1_result.ok());
  
  // Add second member
  auto join2_result = group_manager_->JoinGroup("group1", "member2", topics);
  EXPECT_TRUE(join2_result.ok());
  
  // Should need rebalancing
  EXPECT_TRUE(group_manager_->NeedsRebalancing("group1"));
  
  // Rebalance again
  auto rebalance2_result = group_manager_->RebalanceGroup("group1");
  EXPECT_TRUE(rebalance2_result.ok());
  
  // Should not need rebalancing after rebalance
  EXPECT_FALSE(group_manager_->NeedsRebalancing("group1"));
}

} 
} 
