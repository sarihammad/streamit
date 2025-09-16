#include <gtest/gtest.h>
#include "streamit/storage/record.h"
#include "streamit/storage/serializer.h"
#include <filesystem>
#include <cstdio>

namespace streamit::storage {
namespace {

TEST(RecordTest, SerializeDeserialize) {
  Record record("key1", "value1", 1234567890);
  
  auto serialized = record.Serialize();
  EXPECT_FALSE(serialized.empty());
  
  auto deserialized = Record::Deserialize(serialized);
  EXPECT_EQ(deserialized.key, "key1");
  EXPECT_EQ(deserialized.value, "value1");
  EXPECT_EQ(deserialized.timestamp_ms, 1234567890);
}

TEST(RecordTest, SerializedSize) {
  Record record("key", "value", 1234567890);
  size_t expected_size = sizeof(int32_t) + record.key.size() +  // key length + key data
                        sizeof(int32_t) + record.value.size() + // value length + value data
                        sizeof(int64_t);                        // timestamp
  EXPECT_EQ(record.SerializedSize(), expected_size);
}

TEST(RecordBatchTest, SerializeDeserialize) {
  std::vector<Record> records = {
    Record("key1", "value1", 1234567890),
    Record("key2", "value2", 1234567891)
  };
  
  RecordBatch batch(100, records, 1234567890);
  batch.ComputeCrc32();
  
  auto serialized = batch.Serialize();
  EXPECT_FALSE(serialized.empty());
  
  auto deserialized = RecordBatch::Deserialize(serialized);
  EXPECT_EQ(deserialized.base_offset, 100);
  EXPECT_EQ(deserialized.records.size(), 2);
  EXPECT_EQ(deserialized.records[0].key, "key1");
  EXPECT_EQ(deserialized.records[1].key, "key2");
  EXPECT_TRUE(deserialized.VerifyCrc32());
}

TEST(RecordBatchTest, Crc32Verification) {
  std::vector<Record> records = {
    Record("key1", "value1", 1234567890)
  };
  
  RecordBatch batch(100, records, 1234567890);
  batch.ComputeCrc32();
  
  EXPECT_TRUE(batch.VerifyCrc32());
  
  // Corrupt the data
  batch.records[0].value = "corrupted";
  EXPECT_FALSE(batch.VerifyCrc32());
}

TEST(SerializerTest, SerializeBatch) {
  std::vector<Record> records = {
    Record("key1", "value1", 1234567890)
  };
  
  RecordBatch batch(100, records, 1234567890);
  batch.ComputeCrc32();
  
  auto serialized = Serializer::SerializeBatch(batch);
  EXPECT_FALSE(serialized.empty());
  
  auto deserialized = Serializer::DeserializeBatch(serialized);
  EXPECT_TRUE(deserialized.ok());
  EXPECT_EQ(deserialized.value().base_offset, 100);
  EXPECT_EQ(deserialized.value().records.size(), 1);
}

TEST(SerializerTest, SerializeRecord) {
  Record record("key1", "value1", 1234567890);
  
  auto serialized = Serializer::SerializeRecord(record);
  EXPECT_FALSE(serialized.empty());
  
  auto deserialized = Serializer::DeserializeRecord(serialized);
  EXPECT_TRUE(deserialized.ok());
  EXPECT_EQ(deserialized.value().key, "key1");
  EXPECT_EQ(deserialized.value().value, "value1");
}

TEST(SerializerTest, GetSizes) {
  Record record("key", "value", 1234567890);
  EXPECT_GT(Serializer::GetRecordSize(record), 0);
  
  std::vector<Record> records = {record};
  RecordBatch batch(100, records, 1234567890);
  EXPECT_GT(Serializer::GetBatchSize(batch), 0);
}

} 
} 

