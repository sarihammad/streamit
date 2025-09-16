#include <gtest/gtest.h>
#include "streamit/broker/idempotency_table.h"
#include <string>

namespace streamit::broker {
namespace {

TEST(IdempotencyTableTest, IsValidSequence) {
  IdempotencyTable table;
  
  ProducerKey key{"producer1", "topic1", 0};
  
  // First sequence should be valid
  EXPECT_TRUE(table.IsValidSequence(key, 0));
  
  // Update sequence
  table.UpdateSequence(key, 0, 100);
  
  // Next sequence should be valid
  EXPECT_TRUE(table.IsValidSequence(key, 1));
  
  // Same sequence should be invalid
  EXPECT_FALSE(table.IsValidSequence(key, 1));
  
  // Previous sequence should be invalid
  EXPECT_FALSE(table.IsValidSequence(key, 0));
}

TEST(IdempotencyTableTest, UpdateSequence) {
  IdempotencyTable table;
  
  ProducerKey key{"producer1", "topic1", 0};
  
  table.UpdateSequence(key, 0, 100);
  EXPECT_EQ(table.GetLastSequence(key), 0);
  EXPECT_EQ(table.GetLastOffset(key), 100);
  
  table.UpdateSequence(key, 1, 200);
  EXPECT_EQ(table.GetLastSequence(key), 1);
  EXPECT_EQ(table.GetLastOffset(key), 200);
}

TEST(IdempotencyTableTest, RemoveProducer) {
  IdempotencyTable table;
  
  ProducerKey key1{"producer1", "topic1", 0};
  ProducerKey key2{"producer1", "topic2", 0};
  ProducerKey key3{"producer2", "topic1", 0};
  
  table.UpdateSequence(key1, 0, 100);
  table.UpdateSequence(key2, 0, 200);
  table.UpdateSequence(key3, 0, 300);
  
  EXPECT_EQ(table.Size(), 3);
  
  table.RemoveProducer("producer1");
  
  EXPECT_EQ(table.Size(), 1);
  EXPECT_EQ(table.GetLastSequence(key3), 0);
  EXPECT_EQ(table.GetLastOffset(key3), 300);
}

TEST(IdempotencyTableTest, Clear) {
  IdempotencyTable table;
  
  ProducerKey key{"producer1", "topic1", 0};
  table.UpdateSequence(key, 0, 100);
  
  EXPECT_EQ(table.Size(), 1);
  
  table.Clear();
  
  EXPECT_EQ(table.Size(), 0);
}

TEST(IdempotencyTableTest, EmptyTable) {
  IdempotencyTable table;
  
  ProducerKey key{"producer1", "topic1", 0};
  
  EXPECT_EQ(table.GetLastSequence(key), -1);
  EXPECT_EQ(table.GetLastOffset(key), -1);
}

} 
} 

