#pragma once

#include "streamit/common/result.h"
#include <string>
#include <unordered_map>
#include <deque>
#include <chrono>
#include <cstdint>
#include <mutex>

namespace streamit::broker {

// Key for idempotency table
struct ProducerKey {
  std::string producer_id;
  std::string topic;
  int32_t partition;
  
  bool operator==(const ProducerKey& other) const noexcept {
    return producer_id == other.producer_id && 
           topic == other.topic && 
           partition == other.partition;
  }
};

// Hash function for ProducerKey
struct ProducerKeyHash {
  size_t operator()(const ProducerKey& key) const noexcept {
    return std::hash<std::string>{}(key.producer_id) ^
           (std::hash<std::string>{}(key.topic) << 1) ^
           (std::hash<int32_t>{}(key.partition) << 2);
  }
};

// Value for idempotency table with TTL
struct ProducerState {
  int64_t last_sequence;
  int64_t last_offset;
  std::chrono::steady_clock::time_point timestamp;
  
  ProducerState() = default;
  ProducerState(int64_t seq, int64_t offset) 
    : last_sequence(seq), last_offset(offset), timestamp(std::chrono::steady_clock::now()) {}
  
  bool IsExpired(std::chrono::milliseconds ttl) const noexcept {
    return std::chrono::steady_clock::now() - timestamp > ttl;
  }
};

// Bounded TTL+LRU idempotency table
class BoundedIdempotencyTable {
public:
  // Constructor
  BoundedIdempotencyTable(size_t max_entries, std::chrono::milliseconds ttl);
  
  // Check if a sequence number is valid (not a duplicate)
  [[nodiscard]] bool IsValidSequence(const ProducerKey& key, int64_t sequence) noexcept;
  
  // Update the sequence number and offset for a producer
  [[nodiscard]] void UpdateSequence(const ProducerKey& key, int64_t sequence, int64_t offset) noexcept;
  
  // Get the last sequence number for a producer
  [[nodiscard]] int64_t GetLastSequence(const ProducerKey& key) const noexcept;
  
  // Get the last offset for a producer
  [[nodiscard]] int64_t GetLastOffset(const ProducerKey& key) const noexcept;
  
  // Remove entries for a producer (cleanup)
  [[nodiscard]] void RemoveProducer(const std::string& producer_id) noexcept;
  
  // Get the number of entries
  [[nodiscard]] size_t Size() const noexcept;
  
  // Clear all entries
  [[nodiscard]] void Clear() noexcept;
  
  // Clean up expired entries
  [[nodiscard]] void CleanupExpired() noexcept;

private:
  size_t max_entries_;
  std::chrono::milliseconds ttl_;
  
  // Map for O(1) lookup
  std::unordered_map<ProducerKey, ProducerState, ProducerKeyHash> table_;
  
  // LRU queue for eviction
  std::deque<ProducerKey> lru_queue_;
  
  // Mutex for thread safety
  mutable std::mutex mutex_;
  
  // Helper to evict oldest entry
  [[nodiscard]] void EvictOldest() noexcept;
  
  // Helper to update LRU position
  [[nodiscard]] void UpdateLRU(const ProducerKey& key) noexcept;
};

} // namespace streamit::broker
