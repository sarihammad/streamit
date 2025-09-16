#pragma once

namespace streamit::storage {

// Flush policy for durability guarantees
enum class FlushPolicy {
  Never,      // No fsync (fastest, least durable)
  OnRoll,     // fsync only when segment rolls (balanced)
  EachBatch   // fsync after each batch (most durable)
};

// Convert string to flush policy
[[nodiscard]] FlushPolicy ParseFlushPolicy(const std::string& policy_str) noexcept;

// Convert flush policy to string
[[nodiscard]] std::string ToString(FlushPolicy policy) noexcept;

} // namespace streamit::storage
