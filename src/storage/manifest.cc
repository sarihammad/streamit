#include "streamit/storage/manifest.h"
#include "streamit/common/status.h"
#include <filesystem>
#include <fstream>
#include <sstream>

namespace streamit::storage {

ManifestManager::ManifestManager(std::filesystem::path partition_path) : manifest_path_(std::move(partition_path)) {
}

Result<PartitionManifest> ManifestManager::Load() const noexcept {
  auto manifest_file = GetManifestPath();

  if (!std::filesystem::exists(manifest_file)) {
    return Error<PartitionManifest>(absl::StatusCode::kNotFound, "Manifest file not found");
  }

  std::ifstream file(manifest_file);
  if (!file.is_open()) {
    return Error<PartitionManifest>(absl::StatusCode::kInternal, "Failed to open manifest file");
  }

  PartitionManifest manifest;

  // Simple JSON-like format for now
  std::string line;
  while (std::getline(file, line)) {
    if (line.find("base_offset:") != std::string::npos) {
      std::istringstream iss(line.substr(line.find(":") + 1));
      iss >> manifest.base_offset;
    } else if (line.find("next_offset:") != std::string::npos) {
      std::istringstream iss(line.substr(line.find(":") + 1));
      iss >> manifest.next_offset;
    } else if (line.find("high_watermark:") != std::string::npos) {
      std::istringstream iss(line.substr(line.find(":") + 1));
      iss >> manifest.high_watermark;
    } else if (line.find("timestamp_ms:") != std::string::npos) {
      std::istringstream iss(line.substr(line.find(":") + 1));
      iss >> manifest.timestamp_ms;
    }
  }

  return Ok(std::move(manifest));
}

Result<void> ManifestManager::Save(const PartitionManifest& manifest) noexcept {
  auto manifest_file = GetManifestPath();

  // Ensure directory exists
  std::filesystem::create_directories(manifest_file.parent_path());

  std::ofstream file(manifest_file);
  if (!file.is_open()) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to create manifest file");
  }

  // Simple JSON-like format
  file << "base_offset: " << manifest.base_offset << "\n";
  file << "next_offset: " << manifest.next_offset << "\n";
  file << "high_watermark: " << manifest.high_watermark << "\n";
  file << "timestamp_ms: " << manifest.timestamp_ms << "\n";

  if (file.fail()) {
    return Error<void>(absl::StatusCode::kInternal, "Failed to write manifest file");
  }

  return Ok();
}

Result<void> ManifestManager::UpdateOffsets(int64_t next_offset, int64_t high_watermark) noexcept {
  auto manifest_result = Load();
  if (!manifest_result.ok()) {
    // Create new manifest if it doesn't exist
    PartitionManifest manifest;
    manifest.next_offset = next_offset;
    manifest.high_watermark = high_watermark;
    manifest.timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    return Save(manifest);
  }

  auto manifest = manifest_result.value();
  manifest.next_offset = next_offset;
  manifest.high_watermark = high_watermark;
  manifest.timestamp_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();

  return Save(manifest);
}

bool ManifestManager::Exists() const noexcept {
  return std::filesystem::exists(GetManifestPath());
}

std::filesystem::path ManifestManager::GetManifestPath() const noexcept {
  return manifest_path_ / "MANIFEST";
}

} // namespace streamit::storage
