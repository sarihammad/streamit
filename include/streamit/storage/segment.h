#pragma once

#include "streamit/common/result.h"
#include "streamit/storage/flush_policy.h"
#include "streamit/storage/manifest.h"
#include "streamit/storage/record.h"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>

namespace streamit::storage {

// Segment header for file format
struct SegmentHeader {
  int64_t base_offset;
  int64_t timestamp_ms;
  uint32_t magic; // 0xDEADBEEF
  uint32_t version;

  static constexpr uint32_t kMagic = 0xDEADBEEF;
  static constexpr uint32_t kVersion = 1;
};

// Sparse index entry
struct IndexEntry {
  int64_t relative_offset; // Offset relative to segment base
  int64_t file_position;   // Position in the segment file
  int32_t batch_size;      // Size of the batch at this position

  IndexEntry() = default;
  IndexEntry(int64_t rel_offset, int64_t file_pos, int32_t batch_size)
      : relative_offset(rel_offset), file_position(file_pos), batch_size(batch_size) {
  }
};

// Append-only segment for storing record batches
class Segment {
public:
  // Create a new segment
  Segment(std::filesystem::path log_path, std::filesystem::path index_path, int64_t base_offset, size_t max_size_bytes,
          FlushPolicy flush_policy = FlushPolicy::OnRoll);

  // Open an existing segment
  static Result<std::unique_ptr<Segment>> Open(std::filesystem::path log_path, std::filesystem::path index_path,
                                               FlushPolicy flush_policy = FlushPolicy::OnRoll);

  // Destructor
  ~Segment();

  // Non-copyable, movable
  Segment(const Segment&) = delete;
  Segment& operator=(const Segment&) = delete;
  Segment(Segment&&) noexcept;
  Segment& operator=(Segment&&) noexcept;

  // Append records to the segment
  [[nodiscard]] Result<int64_t> Append(std::span<const Record> records) noexcept;

  // Recover segment from crash (scan tail and truncate if corrupted)
  [[nodiscard]] Result<void> RecoverTail() noexcept;

  // Read batches from the segment
  [[nodiscard]] Result<std::vector<RecordBatch>> Read(int64_t from_offset, size_t max_bytes) const noexcept;

  // Flush data to disk
  [[nodiscard]] Result<void> Flush() noexcept;

  // Flush if needed according to policy
  [[nodiscard]] Result<void> FlushIfNeeded() noexcept;

  // Preallocate space for the segment
  [[nodiscard]] Result<void> Preallocate(size_t size) noexcept;

  // Set file access patterns for performance
  [[nodiscard]] Result<void> SetAccessPattern(bool sequential_write, bool will_need_read) noexcept;

  // Get the end offset of this segment
  [[nodiscard]] int64_t EndOffset() const noexcept;

  // Get the base offset of this segment
  [[nodiscard]] int64_t BaseOffset() const noexcept;

  // Check if the segment is full
  [[nodiscard]] bool IsFull() const noexcept;

  // Check if the segment is closed
  [[nodiscard]] bool IsClosed() const noexcept;

  // Close the segment (no more appends allowed)
  [[nodiscard]] Result<void> Close() noexcept;

  // Get segment size in bytes
  [[nodiscard]] size_t Size() const noexcept;

private:
  std::filesystem::path log_path_;
  std::filesystem::path index_path_;
  int64_t base_offset_;
  size_t max_size_bytes_;
  int64_t end_offset_;
  bool closed_;
  FlushPolicy flush_policy_;
  std::unique_ptr<ManifestManager> manifest_manager_;

  // File handles
  int log_fd_;
  int index_fd_;

  // Current file positions
  int64_t log_position_;
  int64_t index_position_;

  // Index entries (in memory for fast access)
  std::vector<IndexEntry> index_entries_;

  // Mutex for thread safety
  mutable std::mutex mutex_;

  // Private constructor for opening existing segments
  Segment(std::filesystem::path log_path, std::filesystem::path index_path, int64_t base_offset, size_t max_size_bytes,
          int64_t end_offset);

  // Write segment header
  [[nodiscard]] Result<void> WriteHeader() noexcept;

  // Read segment header
  [[nodiscard]] Result<SegmentHeader> ReadHeader() const noexcept;

  // Write index entry
  [[nodiscard]] Result<void> WriteIndexEntry(const IndexEntry& entry) noexcept;

  // Read index entries
  [[nodiscard]] Result<void> LoadIndexEntries() noexcept;

  // Find the index entry for the given offset
  [[nodiscard]] const IndexEntry* FindIndexEntry(int64_t offset) const noexcept;

  // Write data to log file
  [[nodiscard]] Result<void> WriteLogData(std::span<const std::byte> data) noexcept;

  // Read data from log file
  [[nodiscard]] Result<std::vector<std::byte>> ReadLogData(int64_t position, size_t size) const noexcept;
};

} // namespace streamit::storage
