#include "streamit/broker/bounded_idempotency_table.h"
#include <algorithm>

namespace streamit::broker {

BoundedIdempotencyTable::BoundedIdempotencyTable(size_t max_entries, std::chrono::milliseconds ttl)
    : max_entries_(max_entries), ttl_(ttl) {
}

bool BoundedIdempotencyTable::IsValidSequence(const ProducerKey& key, int64_t sequence) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  // Clean up expired entries first
  CleanupExpired();

  auto it = table_.find(key);
  if (it == table_.end()) {
    // New producer, sequence must be 0
    return sequence == 0;
  }

  // Check if sequence is strictly increasing
  return sequence > it->second.last_sequence;
}

void BoundedIdempotencyTable::UpdateSequence(const ProducerKey& key, int64_t sequence, int64_t offset) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  // Clean up expired entries first
  CleanupExpired();

  // Check if we need to evict entries
  while (table_.size() >= max_entries_) {
    EvictOldest();
  }

  // Update or create entry
  auto it = table_.find(key);
  if (it != table_.end()) {
    it->second.last_sequence = sequence;
    it->second.last_offset = offset;
    it->second.timestamp = std::chrono::steady_clock::now();
    UpdateLRU(key);
  } else {
    table_[key] = ProducerState(sequence, offset);
    lru_queue_.push_back(key);
  }
}

int64_t BoundedIdempotencyTable::GetLastSequence(const ProducerKey& key) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = table_.find(key);
  if (it == table_.end()) {
    return -1;
  }

  return it->second.last_sequence;
}

int64_t BoundedIdempotencyTable::GetLastOffset(const ProducerKey& key) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = table_.find(key);
  if (it == table_.end()) {
    return -1;
  }

  return it->second.last_offset;
}

void BoundedIdempotencyTable::RemoveProducer(const std::string& producer_id) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  // Remove all entries for this producer
  auto it = table_.begin();
  while (it != table_.end()) {
    if (it->first.producer_id == producer_id) {
      // Remove from LRU queue
      lru_queue_.erase(std::remove(lru_queue_.begin(), lru_queue_.end(), it->first), lru_queue_.end());

      it = table_.erase(it);
    } else {
      ++it;
    }
  }
}

size_t BoundedIdempotencyTable::Size() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return table_.size();
}

void BoundedIdempotencyTable::Clear() noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  table_.clear();
  lru_queue_.clear();
}

void BoundedIdempotencyTable::CleanupExpired() noexcept {
  auto now = std::chrono::steady_clock::now();

  auto it = table_.begin();
  while (it != table_.end()) {
    if (it->second.IsExpired(ttl_)) {
      // Remove from LRU queue
      lru_queue_.erase(std::remove(lru_queue_.begin(), lru_queue_.end(), it->first), lru_queue_.end());

      it = table_.erase(it);
    } else {
      ++it;
    }
  }
}

void BoundedIdempotencyTable::EvictOldest() noexcept {
  if (lru_queue_.empty()) {
    return;
  }

  const auto& oldest_key = lru_queue_.front();
  table_.erase(oldest_key);
  lru_queue_.pop_front();
}

void BoundedIdempotencyTable::UpdateLRU(const ProducerKey& key) noexcept {
  // Remove from current position
  lru_queue_.erase(std::remove(lru_queue_.begin(), lru_queue_.end(), key), lru_queue_.end());

  // Add to end (most recently used)
  lru_queue_.push_back(key);
}

} // namespace streamit::broker
