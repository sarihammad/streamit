#pragma once

#include <string>
#include <unordered_map>
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

// Value for idempotency table
struct ProducerSequence {
  int64_t last_sequence;
  int64_t last_offset;
  
  ProducerSequence() : last_sequence(-1), last_offset(-1) {}
  ProducerSequence(int64_t seq, int64_t offset) : last_sequence(seq), last_offset(offset) {}
};

// Idempotency table for deduplicating producer requests
class IdempotencyTable {
public:
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

private:
  std::unordered_map<ProducerKey, ProducerSequence, ProducerKeyHash> table_;
  mutable std::mutex mutex_;
};

}

