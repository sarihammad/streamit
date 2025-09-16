#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <span>

namespace streamit::storage {

// A single record in the log
struct Record {
  std::string key;
  std::string value;
  int64_t timestamp_ms;
  
  Record() = default;
  Record(std::string key, std::string value, int64_t timestamp_ms)
    : key(std::move(key)), value(std::move(value)), timestamp_ms(timestamp_ms) {}
  
  // Serialized size estimation
  [[nodiscard]] size_t SerializedSize() const noexcept;
  
  // Serialize to bytes
  [[nodiscard]] std::vector<std::byte> Serialize() const;
  
  // Deserialize from bytes
  [[nodiscard]] static Record Deserialize(std::span<const std::byte> data);
};

// Batch of records with metadata
struct RecordBatch {
  int64_t base_offset;
  std::vector<Record> records;
  int64_t timestamp_ms;
  uint32_t crc32;
  
  RecordBatch() = default;
  RecordBatch(int64_t base_offset, std::vector<Record> records, int64_t timestamp_ms)
    : base_offset(base_offset), records(std::move(records)), timestamp_ms(timestamp_ms) {
    ComputeCrc32();
  }
  
  // Compute and set CRC32
  void ComputeCrc32() noexcept;
  
  // Verify CRC32
  [[nodiscard]] bool VerifyCrc32() const noexcept;
  
  // Serialized size estimation
  [[nodiscard]] size_t SerializedSize() const noexcept;
  
  // Serialize to bytes
  [[nodiscard]] std::vector<std::byte> Serialize() const;
  
  // Deserialize from bytes
  [[nodiscard]] static RecordBatch Deserialize(std::span<const std::byte> data);
};

}

