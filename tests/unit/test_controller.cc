#include <gtest/gtest.h>
#include "streamit/controller/topic_manager.h"
#include <string>

namespace streamit::controller {
namespace {

TEST(TopicManagerTest, CreateTopic) {
  TopicManager manager;
  
  auto result = manager.CreateTopic("test-topic", 3, 1);
  EXPECT_TRUE(result.ok());
  
  EXPECT_TRUE(manager.TopicExists("test-topic"));
  EXPECT_FALSE(manager.TopicExists("non-existent-topic"));
}

TEST(TopicManagerTest, CreateDuplicateTopic) {
  TopicManager manager;
  
  auto result1 = manager.CreateTopic("test-topic", 3, 1);
  EXPECT_TRUE(result1.ok());
  
  auto result2 = manager.CreateTopic("test-topic", 3, 1);
  EXPECT_FALSE(result2.ok());
  EXPECT_EQ(result2.status().code(), absl::StatusCode::kAlreadyExists);
}

TEST(TopicManagerTest, GetTopic) {
  TopicManager manager;
  
  auto create_result = manager.CreateTopic("test-topic", 3, 1);
  EXPECT_TRUE(create_result.ok());
  
  auto get_result = manager.GetTopic("test-topic");
  EXPECT_TRUE(get_result.ok());
  
  const auto& topic_info = get_result.value();
  EXPECT_EQ(topic_info.name, "test-topic");
  EXPECT_EQ(topic_info.partitions, 3);
  EXPECT_EQ(topic_info.replication_factor, 1);
  EXPECT_EQ(topic_info.partition_infos.size(), 3);
}

TEST(TopicManagerTest, GetNonExistentTopic) {
  TopicManager manager;
  
  auto result = manager.GetTopic("non-existent-topic");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kNotFound);
}

TEST(TopicManagerTest, ListTopics) {
  TopicManager manager;
  
  auto result1 = manager.CreateTopic("topic1", 3, 1);
  EXPECT_TRUE(result1.ok());
  
  auto result2 = manager.CreateTopic("topic2", 1, 1);
  EXPECT_TRUE(result2.ok());
  
  auto topics = manager.ListTopics();
  EXPECT_EQ(topics.size(), 2);
  
  // Topics should be sorted
  EXPECT_EQ(topics[0], "topic1");
  EXPECT_EQ(topics[1], "topic2");
}

TEST(TopicManagerTest, DeleteTopic) {
  TopicManager manager;
  
  auto create_result = manager.CreateTopic("test-topic", 3, 1);
  EXPECT_TRUE(create_result.ok());
  
  EXPECT_TRUE(manager.TopicExists("test-topic"));
  
  auto delete_result = manager.DeleteTopic("test-topic");
  EXPECT_TRUE(delete_result.ok());
  
  EXPECT_FALSE(manager.TopicExists("test-topic"));
}

TEST(TopicManagerTest, DeleteNonExistentTopic) {
  TopicManager manager;
  
  auto result = manager.DeleteTopic("non-existent-topic");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kNotFound);
}

TEST(TopicManagerTest, UpdatePartitionLeader) {
  TopicManager manager;
  
  auto create_result = manager.CreateTopic("test-topic", 3, 1);
  EXPECT_TRUE(create_result.ok());
  
  auto update_result = manager.UpdatePartitionLeader("test-topic", 0, 1);
  EXPECT_TRUE(update_result.ok());
  
  auto partition_result = manager.GetPartitionInfo("test-topic", 0);
  EXPECT_TRUE(partition_result.ok());
  EXPECT_EQ(partition_result.value().leader, 1);
}

TEST(TopicManagerTest, UpdatePartitionHighWaterMark) {
  TopicManager manager;
  
  auto create_result = manager.CreateTopic("test-topic", 3, 1);
  EXPECT_TRUE(create_result.ok());
  
  auto update_result = manager.UpdatePartitionHighWaterMark("test-topic", 0, 1000);
  EXPECT_TRUE(update_result.ok());
  
  auto partition_result = manager.GetPartitionInfo("test-topic", 0);
  EXPECT_TRUE(partition_result.ok());
  EXPECT_EQ(partition_result.value().high_watermark, 1000);
}

TEST(TopicManagerTest, GetPartitionInfo) {
  TopicManager manager;
  
  auto create_result = manager.CreateTopic("test-topic", 3, 1);
  EXPECT_TRUE(create_result.ok());
  
  auto partition_result = manager.GetPartitionInfo("test-topic", 0);
  EXPECT_TRUE(partition_result.ok());
  
  const auto& partition_info = partition_result.value();
  EXPECT_EQ(partition_info.partition, 0);
  EXPECT_GE(partition_info.leader, 0);
  EXPECT_FALSE(partition_info.replicas.empty());
}

TEST(TopicManagerTest, GetNonExistentPartition) {
  TopicManager manager;
  
  auto create_result = manager.CreateTopic("test-topic", 3, 1);
  EXPECT_TRUE(create_result.ok());
  
  auto partition_result = manager.GetPartitionInfo("test-topic", 10);
  EXPECT_FALSE(partition_result.ok());
  EXPECT_EQ(partition_result.status().code(), absl::StatusCode::kNotFound);
}

} 
} 

