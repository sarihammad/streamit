#include "streamit/common/crc32.h"
#include <algorithm>
#include <atomic>

namespace streamit::common {

namespace {
// CRC32 lookup table
uint32_t crc_table[256];
std::atomic<bool> table_initialized{false};

void InitializeTable() noexcept {
  for (int i = 0; i < 256; ++i) {
    uint32_t crc = static_cast<uint32_t>(i);
    for (int j = 0; j < 8; ++j) {
      if (crc & 1) {
        crc = (crc >> 1) ^ 0xEDB88320;
      } else {
        crc >>= 1;
      }
    }
    crc_table[i] = crc;
  }
  table_initialized.store(true, std::memory_order_release);
}

bool IsTableInitialized() noexcept {
  return table_initialized.load(std::memory_order_acquire);
}

} // namespace

const uint32_t Crc32::kCrcTable[256] = {};

uint32_t Crc32::Compute(std::span<const std::byte> data) noexcept {
  if (!IsTableInitialized()) {
    InitializeTable();
  }

  uint32_t crc = 0xFFFFFFFF;
  for (const auto& byte : data) {
    crc = crc_table[(crc ^ static_cast<uint8_t>(byte)) & 0xFF] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFF;
}

uint32_t Crc32::Compute(std::string_view data) noexcept {
  return Compute(std::span<const std::byte>(reinterpret_cast<const std::byte*>(data.data()), data.size()));
}

bool Crc32::Verify(std::span<const std::byte> data, uint32_t expected_crc) noexcept {
  return Compute(data) == expected_crc;
}

bool Crc32::Verify(std::string_view data, uint32_t expected_crc) noexcept {
  return Compute(data) == expected_crc;
}

void Crc32::InitializeTable() noexcept {
  ::streamit::common::InitializeTable();
}

bool Crc32::IsTableInitialized() noexcept {
  return ::streamit::common::IsTableInitialized();
}

} // namespace streamit::common
