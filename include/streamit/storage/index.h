#pragma once

#include "streamit/storage/segment.h"
#include "streamit/common/result.h"
#include <filesystem>
#include <vector>
#include <cstdint>

namespace streamit::storage {

// Sparse index for fast offset lookups
class Index {
public:
  // Create a new index
  Index(std::filesystem::path index_path);
  
  // Open an existing index
  static Result<std::unique_ptr<Index>> Open(std::filesystem::path index_path);
  
  // Add an index entry
  [[nodiscard]] Result<void> AddEntry(int64_t relative_offset, int64_t file_position, 
                                     int32_t batch_size) noexcept;
  
  // Find the index entry for the given offset
  [[nodiscard]] Result<IndexEntry> FindEntry(int64_t relative_offset) const noexcept;
  
  // Get all entries in a range
  [[nodiscard]] Result<std::vector<IndexEntry>> GetEntries(int64_t from_offset, 
                                                           int64_t to_offset) const noexcept;
  
  // Flush index to disk
  [[nodiscard]] Result<void> Flush() noexcept;
  
  // Get the number of entries
  [[nodiscard]] size_t Size() const noexcept;
  
  // Check if index is empty
  [[nodiscard]] bool Empty() const noexcept;

private:
  std::filesystem::path index_path_;
  std::vector<IndexEntry> entries_;
  int index_fd_;
  
  // Write index entry to disk
  [[nodiscard]] Result<void> WriteEntry(const IndexEntry& entry) noexcept;
  
  // Load all entries from disk
  [[nodiscard]] Result<void> LoadEntries() noexcept;
};

}

