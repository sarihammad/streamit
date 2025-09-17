#include "streamit/storage/record.h"
#include "streamit/common/crc32.h"
#include <algorithm>
#include <cstring>

namespace streamit::storage {

size_t Record::SerializedSize() const noexcept {
  return sizeof(int32_t) + key.size() +   // key length + key data
         sizeof(int32_t) + value.size() + // value length + value data
         sizeof(int64_t);                 // timestamp
}

std::vector<std::byte> Record::Serialize() const {
  std::vector<std::byte> data;
  data.reserve(SerializedSize());

  // Serialize key
  int32_t key_len = static_cast<int32_t>(key.size());
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&key_len),
              reinterpret_cast<const std::byte*>(&key_len) + sizeof(key_len));
  data.insert(data.end(), reinterpret_cast<const std::byte*>(key.data()),
              reinterpret_cast<const std::byte*>(key.data()) + key.size());

  // Serialize value
  int32_t value_len = static_cast<int32_t>(value.size());
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&value_len),
              reinterpret_cast<const std::byte*>(&value_len) + sizeof(value_len));
  data.insert(data.end(), reinterpret_cast<const std::byte*>(value.data()),
              reinterpret_cast<const std::byte*>(value.data()) + value.size());

  // Serialize timestamp
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&timestamp_ms),
              reinterpret_cast<const std::byte*>(&timestamp_ms) + sizeof(timestamp_ms));

  return data;
}

Record Record::Deserialize(std::span<const std::byte> data) {
  if (data.size() < sizeof(int32_t)) {
    throw std::runtime_error("Invalid record data: too short");
  }

  size_t offset = 0;

  // Deserialize key
  int32_t key_len;
  std::memcpy(&key_len, data.data() + offset, sizeof(key_len));
  offset += sizeof(key_len);

  if (data.size() < offset + key_len) {
    throw std::runtime_error("Invalid record data: key length exceeds data");
  }

  std::string key(reinterpret_cast<const char*>(data.data() + offset), key_len);
  offset += key_len;

  // Deserialize value
  if (data.size() < offset + sizeof(int32_t)) {
    throw std::runtime_error("Invalid record data: missing value length");
  }

  int32_t value_len;
  std::memcpy(&value_len, data.data() + offset, sizeof(value_len));
  offset += sizeof(value_len);

  if (data.size() < offset + value_len) {
    throw std::runtime_error("Invalid record data: value length exceeds data");
  }

  std::string value(reinterpret_cast<const char*>(data.data() + offset), value_len);
  offset += value_len;

  // Deserialize timestamp
  if (data.size() < offset + sizeof(int64_t)) {
    throw std::runtime_error("Invalid record data: missing timestamp");
  }

  int64_t timestamp_ms;
  std::memcpy(&timestamp_ms, data.data() + offset, sizeof(timestamp_ms));

  return Record(std::move(key), std::move(value), timestamp_ms);
}

void RecordBatch::ComputeCrc32() noexcept {
  // Serialize the batch without CRC32 for computing CRC32
  std::vector<std::byte> data;
  data.reserve(SerializedSize() - sizeof(crc32));

  // Serialize base offset
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&base_offset),
              reinterpret_cast<const std::byte*>(&base_offset) + sizeof(base_offset));

  // Serialize timestamp
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&timestamp_ms),
              reinterpret_cast<const std::byte*>(&timestamp_ms) + sizeof(timestamp_ms));

  // Serialize record count
  int32_t record_count = static_cast<int32_t>(records.size());
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&record_count),
              reinterpret_cast<const std::byte*>(&record_count) + sizeof(record_count));

  // Serialize records
  for (const auto& record : records) {
    auto record_data = record.Serialize();
    data.insert(data.end(), record_data.begin(), record_data.end());
  }

  crc32 = streamit::common::Crc32::Compute(data);
}

bool RecordBatch::VerifyCrc32() const noexcept {
  // Serialize the batch without CRC32 for verification
  std::vector<std::byte> data;
  data.reserve(SerializedSize() - sizeof(crc32));

  // Serialize base offset
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&base_offset),
              reinterpret_cast<const std::byte*>(&base_offset) + sizeof(base_offset));

  // Serialize timestamp
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&timestamp_ms),
              reinterpret_cast<const std::byte*>(&timestamp_ms) + sizeof(timestamp_ms));

  // Serialize record count
  int32_t record_count = static_cast<int32_t>(records.size());
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&record_count),
              reinterpret_cast<const std::byte*>(&record_count) + sizeof(record_count));

  // Serialize records
  for (const auto& record : records) {
    auto record_data = record.Serialize();
    data.insert(data.end(), record_data.begin(), record_data.end());
  }

  return streamit::common::Crc32::Compute(data) == crc32;
}

size_t RecordBatch::SerializedSize() const noexcept {
  size_t size = sizeof(base_offset) + sizeof(timestamp_ms) + sizeof(int32_t) + sizeof(crc32);
  for (const auto& record : records) {
    size += record.SerializedSize();
  }
  return size;
}

std::vector<std::byte> RecordBatch::Serialize() const {
  std::vector<std::byte> data;
  data.reserve(SerializedSize());

  // Serialize base offset
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&base_offset),
              reinterpret_cast<const std::byte*>(&base_offset) + sizeof(base_offset));

  // Serialize timestamp
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&timestamp_ms),
              reinterpret_cast<const std::byte*>(&timestamp_ms) + sizeof(timestamp_ms));

  // Serialize record count
  int32_t record_count = static_cast<int32_t>(records.size());
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&record_count),
              reinterpret_cast<const std::byte*>(&record_count) + sizeof(record_count));

  // Serialize records
  for (const auto& record : records) {
    auto record_data = record.Serialize();
    data.insert(data.end(), record_data.begin(), record_data.end());
  }

  // Serialize CRC32
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&crc32),
              reinterpret_cast<const std::byte*>(&crc32) + sizeof(crc32));

  return data;
}

RecordBatch RecordBatch::Deserialize(std::span<const std::byte> data) {
  if (data.size() < sizeof(int64_t) + sizeof(int64_t) + sizeof(int32_t) + sizeof(uint32_t)) {
    throw std::runtime_error("Invalid batch data: too short");
  }

  size_t offset = 0;

  // Deserialize base offset
  int64_t base_offset;
  std::memcpy(&base_offset, data.data() + offset, sizeof(base_offset));
  offset += sizeof(base_offset);

  // Deserialize timestamp
  int64_t timestamp_ms;
  std::memcpy(&timestamp_ms, data.data() + offset, sizeof(timestamp_ms));
  offset += sizeof(timestamp_ms);

  // Deserialize record count
  int32_t record_count;
  std::memcpy(&record_count, data.data() + offset, sizeof(record_count));
  offset += sizeof(record_count);

  // Deserialize records
  std::vector<Record> records;
  records.reserve(record_count);

  for (int32_t i = 0; i < record_count; ++i) {
    // Find the end of this record by parsing its length fields
    if (data.size() < offset + sizeof(int32_t)) {
      throw std::runtime_error("Invalid batch data: missing key length");
    }

    int32_t key_len;
    std::memcpy(&key_len, data.data() + offset, sizeof(key_len));
    offset += sizeof(key_len);

    if (data.size() < offset + key_len + sizeof(int32_t)) {
      throw std::runtime_error("Invalid batch data: key data exceeds bounds");
    }

    offset += key_len; // Skip key data

    int32_t value_len;
    std::memcpy(&value_len, data.data() + offset, sizeof(value_len));
    offset += sizeof(value_len);

    if (data.size() < offset + value_len + sizeof(int64_t)) {
      throw std::runtime_error("Invalid batch data: value data exceeds bounds");
    }

    offset += value_len;       // Skip value data
    offset += sizeof(int64_t); // Skip timestamp

    // Now we know the record size, deserialize it
    size_t record_start = offset - (sizeof(int32_t) + key_len + sizeof(int32_t) + value_len + sizeof(int64_t));
    size_t record_size = sizeof(int32_t) + key_len + sizeof(int32_t) + value_len + sizeof(int64_t);

    auto record_data = data.subspan(record_start, record_size);
    records.push_back(Record::Deserialize(record_data));
  }

  // Deserialize CRC32
  if (data.size() < offset + sizeof(uint32_t)) {
    throw std::runtime_error("Invalid batch data: missing CRC32");
  }

  uint32_t crc32;
  std::memcpy(&crc32, data.data() + offset, sizeof(crc32));

  RecordBatch batch(base_offset, std::move(records), timestamp_ms);
  batch.crc32 = crc32;

  if (!batch.VerifyCrc32()) {
    throw std::runtime_error("Invalid batch data: CRC32 verification failed");
  }

  return batch;
}

} // namespace streamit::storage
