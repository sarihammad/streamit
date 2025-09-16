#include "streamit/storage/segment.h"
#include "streamit/common/status.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <algorithm>
#include <cstring>
#include <chrono>

namespace streamit::storage {

Segment::Segment(std::filesystem::path log_path, std::filesystem::path index_path,
                 int64_t base_offset, size_t max_size_bytes, FlushPolicy flush_policy)
  : log_path_(std::move(log_path))
  , index_path_(std::move(index_path))
  , base_offset_(base_offset)
  , max_size_bytes_(max_size_bytes)
  , end_offset_(base_offset)
  , closed_(false)
  , flush_policy_(flush_policy)
  , log_fd_(-1)
  , index_fd_(-1)
  , log_position_(0)
  , index_position_(0) {
  
  // Create log file
  log_fd_ = open(log_path_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (log_fd_ < 0) {
    throw std::runtime_error("Failed to create log file: " + log_path_.string());
  }
  
  // Create index file
  index_fd_ = open(index_path_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (index_fd_ < 0) {
    close(log_fd_);
    throw std::runtime_error("Failed to create index file: " + index_path_.string());
  }
  
  // Write segment header
  auto header_result = WriteHeader();
  if (!header_result.ok()) {
    close(log_fd_);
    close(index_fd_);
    throw std::runtime_error("Failed to write segment header: " + header_result.status().message());
  }
  
  // Create manifest manager
  manifest_manager_ = std::make_unique<ManifestManager>(log_path_.parent_path());
  
  // Preallocate space for better performance
  auto prealloc_result = Preallocate(max_size_bytes_);
  if (!prealloc_result.ok()) {
    // Log warning but continue
  }
  
  // Set access patterns for performance
  auto pattern_result = SetAccessPattern(true, false); // Sequential write
  if (!pattern_result.ok()) {
    // Log warning but continue
  }
}

Segment::Segment(std::filesystem::path log_path, std::filesystem::path index_path,
                 int64_t base_offset, size_t max_size_bytes, int64_t end_offset, FlushPolicy flush_policy)
  : log_path_(std::move(log_path))
  , index_path_(std::move(index_path))
  , base_offset_(base_offset)
  , max_size_bytes_(max_size_bytes)
  , end_offset_(end_offset)
  , closed_(false)
  , flush_policy_(flush_policy)
  , log_fd_(-1)
  , index_fd_(-1)
  , log_position_(0)
  , index_position_(0) {
  
  // Open log file
  log_fd_ = open(log_path_.c_str(), O_RDWR);
  if (log_fd_ < 0) {
    throw std::runtime_error("Failed to open log file: " + log_path_.string());
  }
  
  // Open index file
  index_fd_ = open(index_path_.c_str(), O_RDWR);
  if (index_fd_ < 0) {
    close(log_fd_);
    throw std::runtime_error("Failed to open index file: " + index_path_.string());
  }
  
  // Load index entries
  auto load_result = LoadIndexEntries();
  if (!load_result.ok()) {
    close(log_fd_);
    close(index_fd_);
    throw std::runtime_error("Failed to load index entries: " + load_result.status().message());
  }
  
  // Get current file positions
  log_position_ = lseek(log_fd_, 0, SEEK_END);
  index_position_ = lseek(index_fd_, 0, SEEK_END);
  
  // Create manifest manager
  manifest_manager_ = std::make_unique<ManifestManager>(log_path_.parent_path());
  
  // Recover from crash if needed
  auto recover_result = RecoverTail();
  if (!recover_result.ok()) {
    close(log_fd_);
    close(index_fd_);
    throw std::runtime_error("Failed to recover segment: " + recover_result.status().message());
  }
}

Segment::~Segment() {
  if (log_fd_ >= 0) {
    close(log_fd_);
  }
  if (index_fd_ >= 0) {
    close(index_fd_);
  }
}

Segment::Segment(Segment&& other) noexcept
  : log_path_(std::move(other.log_path_))
  , index_path_(std::move(other.index_path_))
  , base_offset_(other.base_offset_)
  , max_size_bytes_(other.max_size_bytes_)
  , end_offset_(other.end_offset_)
  , closed_(other.closed_)
  , log_fd_(other.log_fd_)
  , index_fd_(other.index_fd_)
  , log_position_(other.log_position_)
  , index_position_(other.index_position_)
  , index_entries_(std::move(other.index_entries_)) {
  
  other.log_fd_ = -1;
  other.index_fd_ = -1;
}

Segment& Segment::operator=(Segment&& other) noexcept {
  if (this != &other) {
    if (log_fd_ >= 0) close(log_fd_);
    if (index_fd_ >= 0) close(index_fd_);
    
    log_path_ = std::move(other.log_path_);
    index_path_ = std::move(other.index_path_);
    base_offset_ = other.base_offset_;
    max_size_bytes_ = other.max_size_bytes_;
    end_offset_ = other.end_offset_;
    closed_ = other.closed_;
    log_fd_ = other.log_fd_;
    index_fd_ = other.index_fd_;
    log_position_ = other.log_position_;
    index_position_ = other.index_position_;
    index_entries_ = std::move(other.index_entries_);
    
    other.log_fd_ = -1;
    other.index_fd_ = -1;
  }
  return *this;
}

Result<std::unique_ptr<Segment>> Segment::Open(std::filesystem::path log_path,
                                               std::filesystem::path index_path,
                                               FlushPolicy flush_policy) {
  // Read segment header to get base offset
  int log_fd = open(log_path.c_str(), O_RDONLY);
  if (log_fd < 0) {
    return Error<std::unique_ptr<Segment>>(absl::StatusCode::kNotFound,
                                          "Log file not found: " + log_path.string());
  }
  
  SegmentHeader header;
  ssize_t bytes_read = read(log_fd, &header, sizeof(header));
  close(log_fd);
  
  if (bytes_read != sizeof(header)) {
    return Error<std::unique_ptr<Segment>>(absl::StatusCode::kDataLoss,
                                          "Failed to read segment header");
  }
  
  if (header.magic != SegmentHeader::kMagic || header.version != SegmentHeader::kVersion) {
    return Error<std::unique_ptr<Segment>>(absl::StatusCode::kDataLoss,
                                          "Invalid segment header");
  }
  
  // Get file size to determine end offset
  struct stat st;
  if (stat(log_path.c_str(), &st) < 0) {
    return Error<std::unique_ptr<Segment>>(absl::StatusCode::kInternal,
                                          "Failed to stat log file");
  }
  
  // Estimate end offset based on file size (simplified)
  int64_t end_offset = header.base_offset + (st.st_size - sizeof(header)) / 100; // Rough estimate
  
  auto segment = std::make_unique<Segment>(std::move(log_path), std::move(index_path),
                                          header.base_offset, 128 * 1024 * 1024, end_offset, flush_policy);
  return Ok(std::move(segment));
}

Result<int64_t> Segment::Append(std::span<const Record> records) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (closed_) {
    return Error<int64_t>(absl::StatusCode::kFailedPrecondition, "Segment is closed");
  }
  
  if (records.empty()) {
    return Ok(end_offset_);
  }
  
  // Create record batch
  RecordBatch batch(end_offset_, std::vector<Record>(records.begin(), records.end()),
                   std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch()).count());
  
  // Check if segment would be too large
  size_t batch_size = batch.SerializedSize();
  if (log_position_ + batch_size > max_size_bytes_) {
    return Error<int64_t>(absl::StatusCode::kResourceExhausted, "Segment would exceed max size");
  }
  
  // Serialize batch
  auto batch_data = batch.Serialize();
  
  // Write to log file
  auto write_result = WriteLogData(batch_data);
  if (!write_result.ok()) {
    return Error<int64_t>(write_result.status().code(), write_result.status().message());
  }
  
  // Create index entry
  IndexEntry index_entry(end_offset_ - base_offset_, log_position_ - batch_size, batch_size);
  
  // Write index entry
  auto index_result = WriteIndexEntry(index_entry);
  if (!index_result.ok()) {
    return Error<int64_t>(index_result.status().code(), index_result.status().message());
  }
  
  // Update end offset
  int64_t base_offset = end_offset_;
  end_offset_ += records.size();
  
  // Flush if needed according to policy
  auto flush_result = FlushIfNeeded();
  if (!flush_result.ok()) {
    return Error<int64_t>(flush_result.status().code(), flush_result.status().message());
  }
  
  // Update manifest
  if (manifest_manager_) {
    auto manifest_result = manifest_manager_->UpdateOffsets(end_offset_, end_offset_);
    if (!manifest_result.ok()) {
      // Log warning but don't fail the request
    }
  }
  
  return Ok(base_offset);
}

Result<std::vector<RecordBatch>> Segment::Read(int64_t from_offset, size_t max_bytes) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (from_offset < base_offset_ || from_offset >= end_offset_) {
    return Ok(std::vector<RecordBatch>{}); // Empty result for out-of-range
  }
  
  // Find the index entry for the starting offset
  const IndexEntry* index_entry = FindIndexEntry(from_offset);
  if (!index_entry) {
    return Ok(std::vector<RecordBatch>{}); // No data at this offset
  }
  
  std::vector<RecordBatch> batches;
  int64_t current_offset = from_offset;
  size_t bytes_read = 0;
  
  // Read batches starting from the found index entry
  for (size_t i = index_entry - index_entries_.data(); i < index_entries_.size(); ++i) {
    const auto& entry = index_entries_[i];
    
    if (current_offset >= end_offset_) {
      break;
    }
    
    if (bytes_read + entry.batch_size > max_bytes) {
      break;
    }
    
    // Read batch data
    auto batch_data_result = ReadLogData(entry.file_position, entry.batch_size);
    if (!batch_data_result.ok()) {
      return Error<std::vector<RecordBatch>>(batch_data_result.status().code(),
                                            batch_data_result.status().message());
    }
    
    // Deserialize batch
    try {
      auto batch = RecordBatch::Deserialize(batch_data_result.value());
      batches.push_back(std::move(batch));
      bytes_read += entry.batch_size;
      current_offset += batch.records.size();
    } catch (const std::exception& e) {
      return Error<std::vector<RecordBatch>>(absl::StatusCode::kDataLoss,
                                            "Failed to deserialize batch: " + std::string(e.what()));
    }
  }
  
  return Ok(std::move(batches));
}

Result<void> Segment::Flush() noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (fsync(log_fd_) < 0) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to fsync log file");
  }
  
  if (fsync(index_fd_) < 0) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to fsync index file");
  }
  
  return Ok();
}

int64_t Segment::EndOffset() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return end_offset_;
}

int64_t Segment::BaseOffset() const noexcept {
  return base_offset_;
}

bool Segment::IsFull() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return log_position_ >= max_size_bytes_;
}

bool Segment::IsClosed() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return closed_;
}

Result<void> Segment::Close() noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (closed_) {
    return Ok();
  }
  
  // Flush data
  auto flush_result = Flush();
  if (!flush_result.ok()) {
    return flush_result;
  }
  
  closed_ = true;
  return Ok();
}

size_t Segment::Size() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return log_position_;
}

Result<void> Segment::WriteHeader() noexcept {
  SegmentHeader header;
  header.base_offset = base_offset_;
  header.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
  header.magic = SegmentHeader::kMagic;
  header.version = SegmentHeader::kVersion;
  
  ssize_t bytes_written = write(log_fd_, &header, sizeof(header));
  if (bytes_written != sizeof(header)) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to write segment header");
  }
  
  log_position_ += sizeof(header);
  return Ok();
}

Result<SegmentHeader> Segment::ReadHeader() const noexcept {
  SegmentHeader header;
  ssize_t bytes_read = read(log_fd_, &header, sizeof(header));
  if (bytes_read != sizeof(header)) {
    return Error<SegmentHeader>(absl::StatusCode::kDataLoss, "Failed to read segment header");
  }
  return Ok(header);
}

Result<void> Segment::WriteIndexEntry(const IndexEntry& entry) noexcept {
  ssize_t bytes_written = write(index_fd_, &entry, sizeof(entry));
  if (bytes_written != sizeof(entry)) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to write index entry");
  }
  
  index_position_ += sizeof(entry);
  index_entries_.push_back(entry);
  return Ok();
}

Result<void> Segment::LoadIndexEntries() noexcept {
  // Seek to beginning of index file
  if (lseek(index_fd_, 0, SEEK_SET) < 0) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to seek index file");
  }
  
  index_entries_.clear();
  
  // Read all index entries
  IndexEntry entry;
  while (read(index_fd_, &entry, sizeof(entry)) == sizeof(entry)) {
    index_entries_.push_back(entry);
  }
  
  return Ok();
}

const IndexEntry* Segment::FindIndexEntry(int64_t offset) const noexcept {
  int64_t relative_offset = offset - base_offset_;
  
  // Binary search for the index entry
  auto it = std::lower_bound(index_entries_.begin(), index_entries_.end(), relative_offset,
    [](const IndexEntry& entry, int64_t target) {
      return entry.relative_offset < target;
    });
  
  if (it != index_entries_.end() && it->relative_offset <= relative_offset) {
    return &(*it);
  }
  
  return nullptr;
}

Result<void> Segment::WriteLogData(std::span<const std::byte> data) noexcept {
  ssize_t bytes_written = write(log_fd_, data.data(), data.size());
  if (bytes_written != static_cast<ssize_t>(data.size())) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to write log data");
  }
  
  log_position_ += data.size();
  return Ok();
}

Result<std::vector<std::byte>> Segment::ReadLogData(int64_t position, size_t size) const noexcept {
  // Seek to position
  if (lseek(log_fd_, position, SEEK_SET) < 0) {
    return Error<std::vector<std::byte>>(absl::StatusCode::kInternal, "Failed to seek log file");
  }
  
  // Read data
  std::vector<std::byte> data(size);
  ssize_t bytes_read = read(log_fd_, data.data(), size);
  if (bytes_read != static_cast<ssize_t>(size)) {
    return Error<std::vector<std::byte>>(absl::StatusCode::kDataLoss, "Failed to read log data");
  }
  
  return Ok(std::move(data));
}

Result<void> Segment::RecoverTail() noexcept {
  // Get file size
  off_t file_size = lseek(log_fd_, 0, SEEK_END);
  if (file_size < static_cast<off_t>(sizeof(SegmentHeader))) {
    return Ok(); // Empty file, nothing to recover
  }
  
  // Scan from the end backwards to find the last valid batch
  const size_t tail_scan_bytes = 64 * 1024; // Scan last 64KB
  off_t scan_start = std::max<off_t>(sizeof(SegmentHeader), file_size - tail_scan_bytes);
  
  off_t last_valid_pos = sizeof(SegmentHeader);
  
  // Walk through batches from scan_start to end
  for (off_t pos = scan_start; pos < file_size; ) {
    // Read batch header
    RecordBatchHeader header;
    ssize_t bytes_read = pread(log_fd_, &header, sizeof(header), pos);
    if (bytes_read != sizeof(header)) {
      break; // Partial header, truncate here
    }
    
    // Check if this looks like a valid batch header
    if (header.len > 1024 * 1024 || header.len == 0) { // Sanity check
      break; // Invalid length, truncate here
    }
    
    // Check if we have enough data for the full batch
    if (pos + sizeof(header) + header.len > file_size) {
      break; // Partial batch, truncate here
    }
    
    // Verify CRC32
    std::vector<std::byte> batch_data(header.len);
    bytes_read = pread(log_fd_, batch_data.data(), header.len, pos + sizeof(header));
    if (bytes_read != static_cast<ssize_t>(header.len)) {
      break; // Partial data, truncate here
    }
    
    // Compute and verify CRC32
    uint32_t computed_crc = streamit::common::Crc32::Compute(batch_data);
    if (computed_crc != header.crc32) {
      break; // CRC mismatch, truncate here
    }
    
    // This batch is valid, update last valid position
    last_valid_pos = pos + sizeof(header) + header.len;
    
    // Add to index if not already present
    int64_t relative_offset = header.base_offset - base_offset_;
    bool found = false;
    for (const auto& entry : index_entries_) {
      if (entry.relative_offset == relative_offset) {
        found = true;
        break;
      }
    }
    
    if (!found) {
      IndexEntry index_entry(relative_offset, pos, header.len);
      index_entries_.push_back(index_entry);
      
      // Write to index file
      ssize_t bytes_written = write(index_fd_, &index_entry, sizeof(index_entry));
      if (bytes_written != sizeof(index_entry)) {
        // Log warning but continue
      }
    }
    
    pos = last_valid_pos;
  }
  
  // Truncate file if we found corruption
  if (last_valid_pos < file_size) {
    if (ftruncate(log_fd_, last_valid_pos) < 0) {
      return Error<void>(absl::StatusCode::kInternal, "Failed to truncate corrupted segment");
    }
    log_position_ = last_valid_pos;
    
    // Update end offset based on last valid batch
    if (!index_entries_.empty()) {
      const auto& last_entry = index_entries_.back();
      end_offset_ = base_offset_ + last_entry.relative_offset + 1; // +1 for the batch itself
    }
  }
  
  return Ok();
}

Result<void> Segment::FlushIfNeeded() noexcept {
  switch (flush_policy_) {
    case FlushPolicy::Never:
      return Ok();
      
    case FlushPolicy::OnRoll:
      // Only flush when segment is full (handled in caller)
      return Ok();
      
    case FlushPolicy::EachBatch:
      if (fdatasync(log_fd_) < 0) {
        return Error<void>(absl::StatusCode::kInternal, "Failed to fsync log file");
      }
      if (fdatasync(index_fd_) < 0) {
        return Error<void>(absl::StatusCode::kInternal, "Failed to fsync index file");
      }
      return Ok();
      
    default:
      return Ok();
  }
}

Result<void> Segment::Preallocate(size_t size) noexcept {
#ifdef __linux__
  if (posix_fallocate(log_fd_, 0, size) < 0) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to preallocate log file");
  }
  if (posix_fallocate(index_fd_, 0, size / 1024) < 0) { // Index is much smaller
    return Error<void>(absl::StatusCode::kInternal, "Failed to preallocate index file");
  }
#endif
  return Ok();
}

Result<void> Segment::SetAccessPattern(bool sequential_write, bool will_need_read) noexcept {
#ifdef __linux__
  if (sequential_write) {
    if (posix_fadvise(log_fd_, 0, 0, POSIX_FADV_SEQUENTIAL) < 0) {
      return Error<void>(absl::StatusCode::kInternal, "Failed to set sequential write hint");
    }
  }
  
  if (will_need_read) {
    if (posix_fadvise(log_fd_, 0, 0, POSIX_FADV_WILLNEED) < 0) {
      return Error<void>(absl::StatusCode::kInternal, "Failed to set will need read hint");
    }
  }
#endif
  return Ok();
}

}
