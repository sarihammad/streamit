#include "streamit/storage/index.h"
#include "streamit/common/status.h"
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>

namespace streamit::storage {

Index::Index(std::filesystem::path index_path)
  : index_path_(std::move(index_path))
  , index_fd_(-1) {
  
  // Create index file
  index_fd_ = open(index_path_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (index_fd_ < 0) {
    throw std::runtime_error("Failed to create index file: " + index_path_.string());
  }
}

Index::~Index() {
  if (index_fd_ >= 0) {
    close(index_fd_);
  }
}

Result<std::unique_ptr<Index>> Index::Open(std::filesystem::path index_path) {
  // Check if index file exists
  if (!std::filesystem::exists(index_path)) {
    return Error<std::unique_ptr<Index>>(absl::StatusCode::kNotFound,
                                        "Index file not found: " + index_path.string());
  }
  
  auto index = std::make_unique<Index>(index_path);
  
  // Load existing entries
  auto load_result = index->LoadEntries();
  if (!load_result.ok()) {
    return Error<std::unique_ptr<Index>>(load_result.status().code(),
                                        load_result.status().message());
  }
  
  return Ok(std::move(index));
}

Result<void> Index::AddEntry(int64_t relative_offset, int64_t file_position, 
                            int32_t batch_size) noexcept {
  IndexEntry entry(relative_offset, file_position, batch_size);
  
  // Add to memory
  entries_.push_back(entry);
  
  // Write to disk
  auto write_result = WriteEntry(entry);
  if (!write_result.ok()) {
    return write_result;
  }
  
  return Ok();
}

Result<IndexEntry> Index::FindEntry(int64_t relative_offset) const noexcept {
  if (entries_.empty()) {
    return Error<IndexEntry>(absl::StatusCode::kNotFound, "No index entries");
  }
  
  // Binary search for the entry
  auto it = std::lower_bound(entries_.begin(), entries_.end(), relative_offset,
    [](const IndexEntry& entry, int64_t target) {
      return entry.relative_offset < target;
    });
  
  if (it != entries_.end() && it->relative_offset <= relative_offset) {
    return Ok(*it);
  }
  
  return Error<IndexEntry>(absl::StatusCode::kNotFound, "No index entry found for offset");
}

Result<std::vector<IndexEntry>> Index::GetEntries(int64_t from_offset, 
                                                 int64_t to_offset) const noexcept {
  std::vector<IndexEntry> result;
  
  for (const auto& entry : entries_) {
    if (entry.relative_offset >= from_offset && entry.relative_offset < to_offset) {
      result.push_back(entry);
    }
  }
  
  return Ok(std::move(result));
}

Result<void> Index::Flush() noexcept {
  if (fsync(index_fd_) < 0) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to fsync index file");
  }
  return Ok();
}

size_t Index::Size() const noexcept {
  return entries_.size();
}

bool Index::Empty() const noexcept {
  return entries_.empty();
}

Result<void> Index::WriteEntry(const IndexEntry& entry) noexcept {
  ssize_t bytes_written = write(index_fd_, &entry, sizeof(entry));
  if (bytes_written != sizeof(entry)) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to write index entry");
  }
  return Ok();
}

Result<void> Index::LoadEntries() noexcept {
  // Open index file for reading
  int read_fd = open(index_path_.c_str(), O_RDONLY);
  if (read_fd < 0) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to open index file for reading");
  }
  
  entries_.clear();
  
  // Read all entries
  IndexEntry entry;
  while (read(read_fd, &entry, sizeof(entry)) == sizeof(entry)) {
    entries_.push_back(entry);
  }
  
  close(read_fd);
  return Ok();
}

}

