#include <gtest/gtest.h>
#include "streamit/storage/log_dir.h"
#include "streamit/broker/broker_service.h"
#include <filesystem>
#include <thread>
#include <chrono>

namespace streamit::integration {
namespace {

class BrokerIntegrationTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create temporary directory for logs
    log_dir_path_ = std::filesystem::temp_directory_path() / "streamit_test_logs";
    std::filesystem::create_directories(log_dir_path_);
    
    // Create log directory
    log_dir_ = std::make_shared<streamit::storage::LogDir>(log_dir_path_, 128 * 1024 * 1024);
    
    // Create idempotency table
    idempotency_table_ = std::make_shared<streamit::broker::IdempotencyTable>();
    
    // Create broker service
    broker_service_ = std::make_unique<streamit::broker::BrokerServiceImpl>(
      log_dir_, idempotency_table_);
  }
  
  void TearDown() override {
    // Clean up temporary directory
    std::filesystem::remove_all(log_dir_path_);
  }
  
  std::filesystem::path log_dir_path_;
  std::shared_ptr<streamit::storage::LogDir> log_dir_;
  std::shared_ptr<streamit::broker::IdempotencyTable> idempotency_table_;
  std::unique_ptr<streamit::broker::BrokerServiceImpl> broker_service_;
};

TEST_F(BrokerIntegrationTest, ProduceAndFetch) {
  // Create a test topic and partition
  auto segment_result = log_dir_->GetSegment("test-topic", 0);
  ASSERT_TRUE(segment_result.ok());
  
  // Create some test records
  std::vector<streamit::storage::Record> records = {
    streamit::storage::Record("key1", "value1", 1234567890),
    streamit::storage::Record("key2", "value2", 1234567891),
    streamit::storage::Record("key3", "value3", 1234567892)
  };
  
  // Append records to segment
  auto segment = segment_result.value();
  auto append_result = segment->Append(records);
  ASSERT_TRUE(append_result.ok());
  
  int64_t base_offset = append_result.value();
  EXPECT_EQ(base_offset, 0);
  
  // Read records back
  auto read_result = segment->Read(0, 1024 * 1024);
  ASSERT_TRUE(read_result.ok());
  
  const auto& batches = read_result.value();
  EXPECT_EQ(batches.size(), 1);
  EXPECT_EQ(batches[0].records.size(), 3);
  EXPECT_EQ(batches[0].records[0].key, "key1");
  EXPECT_EQ(batches[0].records[1].key, "key2");
  EXPECT_EQ(batches[0].records[2].key, "key3");
}

TEST_F(BrokerIntegrationTest, SegmentRolling) {
  // Create a segment with small max size to force rolling
  auto small_log_dir = std::make_shared<streamit::storage::LogDir>(
    log_dir_path_ / "small", 1024); // 1KB max size
  
  auto segment_result = small_log_dir->GetSegment("test-topic", 0);
  ASSERT_TRUE(segment_result.ok());
  
  auto segment = segment_result.value();
  
  // Add records until segment is full
  std::vector<streamit::storage::Record> records = {
    streamit::storage::Record("key", "value", 1234567890)
  };
  
  int64_t last_offset = 0;
  for (int i = 0; i < 10; ++i) {
    auto append_result = segment->Append(records);
    if (append_result.ok()) {
      last_offset = append_result.value();
    } else {
      // Segment is full, should roll
      break;
    }
  }
  
  // Verify segment is full
  EXPECT_TRUE(segment->IsFull());
}

TEST_F(BrokerIntegrationTest, Idempotency) {
  // Test idempotent producer
  streamit::broker::ProducerKey key{"producer1", "test-topic", 0};
  
  // First sequence should be valid
  EXPECT_TRUE(idempotency_table_->IsValidSequence(key, 0));
  
  // Update sequence
  idempotency_table_->UpdateSequence(key, 0, 100);
  
  // Same sequence should be invalid (duplicate)
  EXPECT_FALSE(idempotency_table_->IsValidSequence(key, 0));
  
  // Next sequence should be valid
  EXPECT_TRUE(idempotency_table_->IsValidSequence(key, 1));
  
  // Previous sequence should be invalid
  EXPECT_FALSE(idempotency_table_->IsValidSequence(key, 0));
}

TEST_F(BrokerIntegrationTest, MultiplePartitions) {
  // Create segments for multiple partitions
  auto segment0_result = log_dir_->GetSegment("test-topic", 0);
  ASSERT_TRUE(segment0_result.ok());
  
  auto segment1_result = log_dir_->GetSegment("test-topic", 1);
  ASSERT_TRUE(segment1_result.ok());
  
  // Add records to different partitions
  std::vector<streamit::storage::Record> records0 = {
    streamit::storage::Record("key0", "value0", 1234567890)
  };
  
  std::vector<streamit::storage::Record> records1 = {
    streamit::storage::Record("key1", "value1", 1234567891)
  };
  
  auto append0_result = segment0_result.value()->Append(records0);
  ASSERT_TRUE(append0_result.ok());
  
  auto append1_result = segment1_result.value()->Append(records1);
  ASSERT_TRUE(append1_result.ok());
  
  // Verify records are in correct partitions
  auto read0_result = segment0_result.value()->Read(0, 1024);
  ASSERT_TRUE(read0_result.ok());
  EXPECT_EQ(read0_result.value()[0].records[0].key, "key0");
  
  auto read1_result = segment1_result.value()->Read(0, 1024);
  ASSERT_TRUE(read1_result.ok());
  EXPECT_EQ(read1_result.value()[0].records[0].key, "key1");
}

} 
} 

