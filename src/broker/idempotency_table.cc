#include "streamit/broker/idempotency_table.h"

namespace streamit::broker {

bool IdempotencyTable::IsValidSequence(const ProducerKey& key, int64_t sequence) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = table_.find(key);
  if (it == table_.end()) {
    // New producer, sequence must be 0
    return sequence == 0;
  }

  // Check if sequence is strictly increasing
  return sequence > it->second.last_sequence;
}

void IdempotencyTable::UpdateSequence(const ProducerKey& key, int64_t sequence, int64_t offset) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  table_[key] = ProducerSequence(sequence, offset);
}

int64_t IdempotencyTable::GetLastSequence(const ProducerKey& key) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = table_.find(key);
  if (it == table_.end()) {
    return -1;
  }

  return it->second.last_sequence;
}

int64_t IdempotencyTable::GetLastOffset(const ProducerKey& key) const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = table_.find(key);
  if (it == table_.end()) {
    return -1;
  }

  return it->second.last_offset;
}

void IdempotencyTable::RemoveProducer(const std::string& producer_id) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);

  // Remove all entries for this producer
  auto it = table_.begin();
  while (it != table_.end()) {
    if (it->first.producer_id == producer_id) {
      it = table_.erase(it);
    } else {
      ++it;
    }
  }
}

size_t IdempotencyTable::Size() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return table_.size();
}

void IdempotencyTable::Clear() noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  table_.clear();
}

} // namespace streamit::broker
