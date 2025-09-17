#include "streamit/storage/serializer.h"
#include "streamit/common/status.h"
#include <algorithm>
#include <cstring>

namespace streamit::storage {

std::vector<std::byte> Serializer::SerializeBatch(const RecordBatch& batch) noexcept {
  std::vector<std::byte> data;
  data.reserve(GetBatchSize(batch));

  // Write batch header
  WriteInt64(data, batch.base_offset);
  WriteInt64(data, batch.timestamp_ms);
  WriteInt32(data, static_cast<int32_t>(batch.records.size()));

  // Write records
  for (const auto& record : batch.records) {
    auto record_data = SerializeRecord(record);
    data.insert(data.end(), record_data.begin(), record_data.end());
  }

  // Write CRC32
  WriteUint32(data, batch.crc32);

  return data;
}

Result<RecordBatch> Serializer::DeserializeBatch(std::span<const std::byte> data) noexcept {
  if (data.size() < sizeof(int64_t) + sizeof(int64_t) + sizeof(int32_t) + sizeof(uint32_t)) {
    return Error<RecordBatch>(absl::StatusCode::kInvalidArgument, "Data too short for batch");
  }

  std::span<const std::byte> remaining_data = data;

  // Read batch header
  auto base_offset_result = ReadInt64(remaining_data);
  if (!base_offset_result.ok())
    return Error<RecordBatch>(base_offset_result.status());

  auto timestamp_result = ReadInt64(remaining_data);
  if (!timestamp_result.ok())
    return Error<RecordBatch>(timestamp_result.status());

  auto record_count_result = ReadInt32(remaining_data);
  if (!record_count_result.ok())
    return Error<RecordBatch>(record_count_result.status());

  int64_t base_offset = base_offset_result.value();
  int64_t timestamp_ms = timestamp_result.value();
  int32_t record_count = record_count_result.value();

  // Read records
  std::vector<Record> records;
  records.reserve(record_count);

  for (int32_t i = 0; i < record_count; ++i) {
    auto record_result = DeserializeRecord(remaining_data);
    if (!record_result.ok()) {
      return Error<RecordBatch>(record_result.status());
    }
    records.push_back(std::move(record_result.value()));
  }

  // Read CRC32
  auto crc32_result = ReadUint32(remaining_data);
  if (!crc32_result.ok())
    return Error<RecordBatch>(crc32_result.status());

  uint32_t crc32 = crc32_result.value();

  // Create batch
  RecordBatch batch(base_offset, std::move(records), timestamp_ms);
  batch.crc32 = crc32;

  // Verify CRC32
  if (!batch.VerifyCrc32()) {
    return Error<RecordBatch>(absl::StatusCode::kDataLoss, "CRC32 verification failed");
  }

  return Ok(std::move(batch));
}

std::vector<std::byte> Serializer::SerializeRecord(const Record& record) noexcept {
  std::vector<std::byte> data;
  data.reserve(GetRecordSize(record));

  // Write key
  WriteInt32(data, static_cast<int32_t>(record.key.size()));
  data.insert(data.end(), reinterpret_cast<const std::byte*>(record.key.data()),
              reinterpret_cast<const std::byte*>(record.key.data()) + record.key.size());

  // Write value
  WriteInt32(data, static_cast<int32_t>(record.value.size()));
  data.insert(data.end(), reinterpret_cast<const std::byte*>(record.value.data()),
              reinterpret_cast<const std::byte*>(record.value.data()) + record.value.size());

  // Write timestamp
  WriteInt64(data, record.timestamp_ms);

  return data;
}

Result<Record> Serializer::DeserializeRecord(std::span<const std::byte> data) noexcept {
  if (data.size() < sizeof(int32_t)) {
    return Error<Record>(absl::StatusCode::kInvalidArgument, "Data too short for record");
  }

  std::span<const std::byte> remaining_data = data;

  // Read key
  auto key_len_result = ReadInt32(remaining_data);
  if (!key_len_result.ok())
    return Error<Record>(key_len_result.status());

  int32_t key_len = key_len_result.value();
  if (remaining_data.size() < static_cast<size_t>(key_len)) {
    return Error<Record>(absl::StatusCode::kInvalidArgument, "Key length exceeds data");
  }

  std::string key(reinterpret_cast<const char*>(remaining_data.data()), key_len);
  remaining_data = remaining_data.subspan(key_len);

  // Read value
  if (remaining_data.size() < sizeof(int32_t)) {
    return Error<Record>(absl::StatusCode::kInvalidArgument, "Missing value length");
  }

  auto value_len_result = ReadInt32(remaining_data);
  if (!value_len_result.ok())
    return Error<Record>(value_len_result.status());

  int32_t value_len = value_len_result.value();
  if (remaining_data.size() < static_cast<size_t>(value_len)) {
    return Error<Record>(absl::StatusCode::kInvalidArgument, "Value length exceeds data");
  }

  std::string value(reinterpret_cast<const char*>(remaining_data.data()), value_len);
  remaining_data = remaining_data.subspan(value_len);

  // Read timestamp
  if (remaining_data.size() < sizeof(int64_t)) {
    return Error<Record>(absl::StatusCode::kInvalidArgument, "Missing timestamp");
  }

  auto timestamp_result = ReadInt64(remaining_data);
  if (!timestamp_result.ok())
    return Error<Record>(timestamp_result.status());

  return Ok(Record(std::move(key), std::move(value), timestamp_result.value()));
}

size_t Serializer::GetBatchSize(const RecordBatch& batch) noexcept {
  size_t size = sizeof(int64_t) + sizeof(int64_t) + sizeof(int32_t) + sizeof(uint32_t); // Header + CRC32
  for (const auto& record : batch.records) {
    size += GetRecordSize(record);
  }
  return size;
}

size_t Serializer::GetRecordSize(const Record& record) noexcept {
  return sizeof(int32_t) + record.key.size() +   // key length + key data
         sizeof(int32_t) + record.value.size() + // value length + value data
         sizeof(int64_t);                        // timestamp
}

void Serializer::WriteInt32(std::vector<std::byte>& data, int32_t value) noexcept {
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&value),
              reinterpret_cast<const std::byte*>(&value) + sizeof(value));
}

void Serializer::WriteInt64(std::vector<std::byte>& data, int64_t value) noexcept {
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&value),
              reinterpret_cast<const std::byte*>(&value) + sizeof(value));
}

void Serializer::WriteUint32(std::vector<std::byte>& data, uint32_t value) noexcept {
  data.insert(data.end(), reinterpret_cast<const std::byte*>(&value),
              reinterpret_cast<const std::byte*>(&value) + sizeof(value));
}

Result<int32_t> Serializer::ReadInt32(std::span<const std::byte>& data) noexcept {
  if (data.size() < sizeof(int32_t)) {
    return Error<int32_t>(absl::StatusCode::kInvalidArgument, "Not enough data for int32");
  }

  int32_t value;
  std::memcpy(&value, data.data(), sizeof(value));
  data = data.subspan(sizeof(value));
  return Ok(value);
}

Result<int64_t> Serializer::ReadInt64(std::span<const std::byte>& data) noexcept {
  if (data.size() < sizeof(int64_t)) {
    return Error<int64_t>(absl::StatusCode::kInvalidArgument, "Not enough data for int64");
  }

  int64_t value;
  std::memcpy(&value, data.data(), sizeof(value));
  data = data.subspan(sizeof(value));
  return Ok(value);
}

Result<uint32_t> Serializer::ReadUint32(std::span<const std::byte>& data) noexcept {
  if (data.size() < sizeof(uint32_t)) {
    return Error<uint32_t>(absl::StatusCode::kInvalidArgument, "Not enough data for uint32");
  }

  uint32_t value;
  std::memcpy(&value, data.data(), sizeof(value));
  data = data.subspan(sizeof(value));
  return Ok(value);
}

} // namespace streamit::storage
