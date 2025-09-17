#pragma once

#include "streamit/common/result.h"
#include <cstdint>
#include <filesystem>

namespace streamit::storage {

// Manifest file for partition metadata
struct PartitionManifest {
  int64_t base_offset;
  int64_t next_offset;
  int64_t high_watermark;
  int64_t timestamp_ms;

  PartitionManifest() = default;
  PartitionManifest(int64_t base_offset, int64_t next_offset, int64_t high_watermark, int64_t timestamp_ms)
      : base_offset(base_offset), next_offset(next_offset), high_watermark(high_watermark), timestamp_ms(timestamp_ms) {
  }
};

// Manifest manager for partition metadata persistence
class ManifestManager {
public:
  // Constructor
  ManifestManager(std::filesystem::path partition_path);

  // Load manifest from disk
  [[nodiscard]] Result<PartitionManifest> Load() const noexcept;

  // Save manifest to disk
  [[nodiscard]] Result<void> Save(const PartitionManifest& manifest) noexcept;

  // Update manifest with new offsets
  [[nodiscard]] Result<void> UpdateOffsets(int64_t next_offset, int64_t high_watermark) noexcept;

  // Check if manifest exists
  [[nodiscard]] bool Exists() const noexcept;

private:
  std::filesystem::path manifest_path_;

  // Get manifest file path
  [[nodiscard]] std::filesystem::path GetManifestPath() const noexcept;
};

} // namespace streamit::storage
