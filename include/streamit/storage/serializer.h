#pragma once

#include "streamit/common/result.h"
#include "streamit/storage/record.h"
#include <cstdint>
#include <span>
#include <vector>

namespace streamit::storage {

// Serialization utilities for record batches
class Serializer {
public:
  // Serialize a record batch to bytes
  [[nodiscard]] static std::vector<std::byte> SerializeBatch(const RecordBatch& batch) noexcept;

  // Deserialize bytes to a record batch
  [[nodiscard]] static Result<RecordBatch> DeserializeBatch(std::span<const std::byte> data) noexcept;

  // Serialize a single record to bytes
  [[nodiscard]] static std::vector<std::byte> SerializeRecord(const Record& record) noexcept;

  // Deserialize bytes to a single record
  [[nodiscard]] static Result<Record> DeserializeRecord(std::span<const std::byte> data) noexcept;

  // Compute the serialized size of a record batch
  [[nodiscard]] static size_t GetBatchSize(const RecordBatch& batch) noexcept;

  // Compute the serialized size of a record
  [[nodiscard]] static size_t GetRecordSize(const Record& record) noexcept;

private:
  // Helper to write a 32-bit integer in little-endian format
  static void WriteInt32(std::vector<std::byte>& data, int32_t value) noexcept;

  // Helper to write a 64-bit integer in little-endian format
  static void WriteInt64(std::vector<std::byte>& data, int64_t value) noexcept;

  // Helper to write a 32-bit unsigned integer in little-endian format
  static void WriteUint32(std::vector<std::byte>& data, uint32_t value) noexcept;

  // Helper to read a 32-bit integer in little-endian format
  static Result<int32_t> ReadInt32(std::span<const std::byte>& data) noexcept;

  // Helper to read a 64-bit integer in little-endian format
  static Result<int64_t> ReadInt64(std::span<const std::byte>& data) noexcept;

  // Helper to read a 32-bit unsigned integer in little-endian format
  static Result<uint32_t> ReadUint32(std::span<const std::byte>& data) noexcept;
};

} // namespace streamit::storage
