#pragma once

#include <cstdint>
#include <span>

namespace streamit::common {

// CRC32 implementation for data integrity
class Crc32 {
public:
  // Compute CRC32 of data
  [[nodiscard]] static uint32_t Compute(std::span<const std::byte> data) noexcept;
  
  // Compute CRC32 of string data
  [[nodiscard]] static uint32_t Compute(std::string_view data) noexcept;
  
  // Verify CRC32 of data
  [[nodiscard]] static bool Verify(std::span<const std::byte> data, uint32_t expected_crc) noexcept;
  
  // Verify CRC32 of string data
  [[nodiscard]] static bool Verify(std::string_view data, uint32_t expected_crc) noexcept;

private:
  // CRC32 lookup table
  static const uint32_t kCrcTable[256];
  
  // Initialize CRC32 table
  static void InitializeTable() noexcept;
  
  // Check if table is initialized
  static bool IsTableInitialized() noexcept;
};

}

