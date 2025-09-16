#include "streamit/storage/log_dir.h"
#include "streamit/common/status.h"
#include <filesystem>
#include <algorithm>
#include <fstream>

namespace streamit::storage {

LogDir::LogDir(std::filesystem::path root_path, size_t max_segment_size_bytes)
  : root_path_(std::move(root_path))
  , max_segment_size_bytes_(max_segment_size_bytes) {
  
  // Create root directory if it doesn't exist
  std::filesystem::create_directories(root_path_);
}

Result<std::unique_ptr<LogDir>> LogDir::Open(std::filesystem::path root_path, 
                                            size_t max_segment_size_bytes) {
  if (!std::filesystem::exists(root_path)) {
    return Error<std::unique_ptr<LogDir>>(absl::StatusCode::kNotFound,
                                         "Log directory not found: " + root_path.string());
  }
  
  auto log_dir = std::make_unique<LogDir>(std::move(root_path), max_segment_size_bytes);
  
  // Load existing topics and partitions
  for (const auto& topic_entry : std::filesystem::directory_iterator(log_dir->root_path_)) {
    if (!topic_entry.is_directory()) continue;
    
    std::string topic = topic_entry.path().filename().string();
    
    for (const auto& partition_entry : std::filesystem::directory_iterator(topic_entry.path())) {
      if (!partition_entry.is_directory()) continue;
      
      std::string partition_str = partition_entry.path().filename().string();
      try {
        int32_t partition = std::stoi(partition_str);
        
        // Load segments for this partition
        auto load_result = log_dir->LoadSegments(topic, partition);
        if (!load_result.ok()) {
          return Error<std::unique_ptr<LogDir>>(load_result.status().code(),
                                               load_result.status().message());
        }
      } catch (const std::exception&) {
        // Skip invalid partition directories
        continue;
      }
    }
  }
  
  return Ok(std::move(log_dir));
}

Result<std::shared_ptr<Segment>> LogDir::GetSegment(const std::string& topic, 
                                                   int32_t partition) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Check if we have an active segment
  auto active_result = GetActiveSegment(topic, partition);
  if (active_result.ok() && !active_result.value()->IsFull() && !active_result.value()->IsClosed()) {
    return active_result;
  }
  
  // Create a new segment
  return RollSegment(topic, partition);
}

Result<std::vector<std::shared_ptr<Segment>>> LogDir::GetSegments(
  const std::string& topic, int32_t partition) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto topic_it = segments_.find(topic);
  if (topic_it == segments_.end()) {
    return Ok(std::vector<std::shared_ptr<Segment>>{});
  }
  
  auto partition_it = topic_it->second.find(partition);
  if (partition_it == topic_it->second.end()) {
    return Ok(std::vector<std::shared_ptr<Segment>>{});
  }
  
  return Ok(partition_it->second);
}

Result<std::shared_ptr<Segment>> LogDir::GetActiveSegment(
  const std::string& topic, int32_t partition) const noexcept {
  auto segments_result = GetSegments(topic, partition);
  if (!segments_result.ok()) {
    return Error<std::shared_ptr<Segment>>(segments_result.status().code(),
                                          segments_result.status().message());
  }
  
  const auto& segments = segments_result.value();
  if (segments.empty()) {
    return Error<std::shared_ptr<Segment>>(absl::StatusCode::kNotFound, "No segments found");
  }
  
  // Return the last (most recent) segment
  return Ok(segments.back());
}

Result<std::shared_ptr<Segment>> LogDir::RollSegment(const std::string& topic, 
                                                    int32_t partition) noexcept {
  // Get current end offset
  auto end_offset_result = GetEndOffset(topic, partition);
  if (!end_offset_result.ok()) {
    return Error<std::shared_ptr<Segment>>(end_offset_result.status().code(),
                                          end_offset_result.status().message());
  }
  
  int64_t base_offset = end_offset_result.value();
  
  // Create new segment
  auto segment_result = CreateSegment(topic, partition, base_offset);
  if (!segment_result.ok()) {
    return segment_result;
  }
  
  auto segment = segment_result.value();
  
  // Add to segments map
  segments_[topic][partition].push_back(segment);
  
  return Ok(segment);
}

Result<int64_t> LogDir::GetEndOffset(const std::string& topic, 
                                    int32_t partition) const noexcept {
  auto segments_result = GetSegments(topic, partition);
  if (!segments_result.ok()) {
    return Error<int64_t>(segments_result.status().code(), segments_result.status().message());
  }
  
  const auto& segments = segments_result.value();
  if (segments.empty()) {
    return Ok(0);
  }
  
  // Return the end offset of the last segment
  return Ok(segments.back()->EndOffset());
}

Result<int64_t> LogDir::GetHighWaterMark(const std::string& topic, 
                                         int32_t partition) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto topic_it = high_water_marks_.find(topic);
  if (topic_it == high_water_marks_.end()) {
    return Ok(0);
  }
  
  auto partition_it = topic_it->second.find(partition);
  if (partition_it == topic_it->second.end()) {
    return Ok(0);
  }
  
  return Ok(partition_it->second);
}

Result<void> LogDir::SetHighWaterMark(const std::string& topic, 
                                      int32_t partition, int64_t offset) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  
  high_water_marks_[topic][partition] = offset;
  
  // Persist to disk (simplified - in practice would write to a metadata file)
  auto partition_path = GetPartitionPath(topic, partition);
  std::filesystem::create_directories(partition_path);
  
  auto hwm_path = partition_path / "high_water_mark";
  std::ofstream file(hwm_path);
  if (file.is_open()) {
    file << offset;
  }
  
  return Ok();
}

std::vector<std::string> LogDir::ListTopics() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<std::string> topics;
  for (const auto& topic_entry : segments_) {
    topics.push_back(topic_entry.first);
  }
  
  return topics;
}

Result<std::vector<int32_t>> LogDir::ListPartitions(const std::string& topic) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto topic_it = segments_.find(topic);
  if (topic_it == segments_.end()) {
    return Ok(std::vector<int32_t>{});
  }
  
  std::vector<int32_t> partitions;
  for (const auto& partition_entry : topic_it->second) {
    partitions.push_back(partition_entry.first);
  }
  
  std::sort(partitions.begin(), partitions.end());
  return Ok(std::move(partitions));
}

Result<void> LogDir::CleanupOldSegments(const std::string& topic, 
                                        int32_t partition, 
                                        int64_t retention_bytes) noexcept {
  auto segments_result = GetSegments(topic, partition);
  if (!segments_result.ok()) {
    return segments_result;
  }
  
  auto& segments = segments_result.value();
  if (segments.size() <= 1) {
    return Ok(); // Keep at least one segment
  }
  
  // Calculate total size and remove old segments
  size_t total_size = 0;
  size_t segments_to_keep = 1; // Always keep the last segment
  
  for (auto it = segments.rbegin() + 1; it != segments.rend(); ++it) {
    total_size += (*it)->Size();
    if (total_size > static_cast<size_t>(retention_bytes)) {
      break;
    }
    ++segments_to_keep;
  }
  
  // Remove old segments
  size_t segments_to_remove = segments.size() - segments_to_keep;
  if (segments_to_remove > 0) {
    std::lock_guard<std::mutex> lock(mutex_);
    segments.erase(segments.begin(), segments.begin() + segments_to_remove);
  }
  
  return Ok();
}

std::filesystem::path LogDir::GetPartitionPath(const std::string& topic, 
                                              int32_t partition) const noexcept {
  return root_path_ / topic / std::to_string(partition);
}

Result<void> LogDir::LoadSegments(const std::string& topic, int32_t partition) noexcept {
  auto partition_path = GetPartitionPath(topic, partition);
  if (!std::filesystem::exists(partition_path)) {
    return Ok(); // No segments to load
  }
  
  std::vector<std::shared_ptr<Segment>> segments;
  
  // Find all segment files
  for (const auto& entry : std::filesystem::directory_iterator(partition_path)) {
    if (!entry.is_regular_file()) continue;
    
    std::string filename = entry.path().filename().string();
    if (filename.ends_with(".log")) {
      std::string segment_name = filename.substr(0, filename.length() - 4);
      auto log_path = entry.path();
      auto index_path = entry.path().parent_path() / (segment_name + ".index");
      
      if (std::filesystem::exists(index_path)) {
        auto segment_result = Segment::Open(log_path, index_path);
        if (segment_result.ok()) {
          segments.push_back(std::move(segment_result.value()));
        }
      }
    }
  }
  
  // Sort segments by base offset
  std::sort(segments.begin(), segments.end(),
    [](const std::shared_ptr<Segment>& a, const std::shared_ptr<Segment>& b) {
      return a->BaseOffset() < b->BaseOffset();
    });
  
  // Add to segments map
  segments_[topic][partition] = std::move(segments);
  
  return Ok();
}

Result<std::shared_ptr<Segment>> LogDir::CreateSegment(const std::string& topic, 
                                                      int32_t partition, 
                                                      int64_t base_offset) noexcept {
  auto partition_path = GetPartitionPath(topic, partition);
  std::filesystem::create_directories(partition_path);
  
  int64_t segment_number = GetNextSegmentNumber(topic, partition);
  std::string segment_name = std::to_string(segment_number);
  
  auto log_path = partition_path / (segment_name + ".log");
  auto index_path = partition_path / (segment_name + ".index");
  
  try {
    auto segment = std::make_shared<Segment>(log_path, index_path, base_offset, max_segment_size_bytes_);
    return Ok(std::move(segment));
  } catch (const std::exception& e) {
    return Error<std::shared_ptr<Segment>>(absl::StatusCode::kInternal,
                                          "Failed to create segment: " + std::string(e.what()));
  }
}

int64_t LogDir::GetNextSegmentNumber(const std::string& topic, 
                                    int32_t partition) const noexcept {
  auto segments_result = GetSegments(topic, partition);
  if (!segments_result.ok() || segments_result.value().empty()) {
    return 0;
  }
  
  // Get the highest segment number and increment
  int64_t max_segment = 0;
  for (const auto& segment : segments_result.value()) {
    // Extract segment number from path (simplified)
    max_segment = std::max(max_segment, segment->BaseOffset() / 1000); // Rough estimation
  }
  
  return max_segment + 1;
}

}

