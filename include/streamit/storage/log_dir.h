#pragma once

#include "streamit/storage/segment.h"
#include "streamit/common/result.h"
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <cstdint>

namespace streamit::storage {

// Log directory management for topics and partitions
class LogDir {
public:
  // Create a new log directory
  LogDir(std::filesystem::path root_path, size_t max_segment_size_bytes);
  
  // Open an existing log directory
  static Result<std::unique_ptr<LogDir>> Open(std::filesystem::path root_path, 
                                             size_t max_segment_size_bytes);
  
  // Get or create a segment for the given topic and partition
  [[nodiscard]] Result<std::shared_ptr<Segment>> GetSegment(const std::string& topic, 
                                                           int32_t partition) noexcept;
  
  // Get all segments for a topic and partition
  [[nodiscard]] Result<std::vector<std::shared_ptr<Segment>>> GetSegments(
    const std::string& topic, int32_t partition) const noexcept;
  
  // Get the current active segment for a topic and partition
  [[nodiscard]] Result<std::shared_ptr<Segment>> GetActiveSegment(
    const std::string& topic, int32_t partition) const noexcept;
  
  // Roll the current segment for a topic and partition
  [[nodiscard]] Result<std::shared_ptr<Segment>> RollSegment(
    const std::string& topic, int32_t partition) noexcept;
  
  // Get the end offset for a topic and partition
  [[nodiscard]] Result<int64_t> GetEndOffset(const std::string& topic, 
                                            int32_t partition) const noexcept;
  
  // Get the high water mark for a topic and partition
  [[nodiscard]] Result<int64_t> GetHighWaterMark(const std::string& topic, 
                                                 int32_t partition) const noexcept;
  
  // Set the high water mark for a topic and partition
  [[nodiscard]] Result<void> SetHighWaterMark(const std::string& topic, 
                                             int32_t partition, int64_t offset) noexcept;
  
  // List all topics
  [[nodiscard]] std::vector<std::string> ListTopics() const noexcept;
  
  // List all partitions for a topic
  [[nodiscard]] Result<std::vector<int32_t>> ListPartitions(const std::string& topic) const noexcept;
  
  // Clean up old segments (retention policy)
  [[nodiscard]] Result<void> CleanupOldSegments(const std::string& topic, 
                                               int32_t partition, 
                                               int64_t retention_bytes) noexcept;

private:
  std::filesystem::path root_path_;
  size_t max_segment_size_bytes_;
  
  // Topic -> Partition -> Segments
  std::unordered_map<std::string, std::unordered_map<int32_t, std::vector<std::shared_ptr<Segment>>>> segments_;
  
  // Topic -> Partition -> High Water Mark
  std::unordered_map<std::string, std::unordered_map<int32_t, int64_t>> high_water_marks_;
  
  // Mutex for thread safety
  mutable std::mutex mutex_;
  
  // Get the directory path for a topic and partition
  [[nodiscard]] std::filesystem::path GetPartitionPath(const std::string& topic, 
                                                       int32_t partition) const noexcept;
  
  // Load existing segments for a topic and partition
  [[nodiscard]] Result<void> LoadSegments(const std::string& topic, int32_t partition) noexcept;
  
  // Create a new segment for a topic and partition
  [[nodiscard]] Result<std::shared_ptr<Segment>> CreateSegment(
    const std::string& topic, int32_t partition, int64_t base_offset) noexcept;
  
  // Get the next segment number for a topic and partition
  [[nodiscard]] int64_t GetNextSegmentNumber(const std::string& topic, 
                                            int32_t partition) const noexcept;
};

}

