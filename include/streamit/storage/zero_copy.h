#pragma once

#include <cstddef>
#include <cstdint>

namespace streamit::storage {

// Zero-copy utilities for efficient data transfer
class ZeroCopy {
public:
  // Send data from file descriptor to socket using zero-copy
  // Returns number of bytes sent, or -1 on error
  [[nodiscard]] static ssize_t SendFile(int out_fd, int in_fd, off_t* offset, size_t count) noexcept;
  
  // Check if zero-copy is available on this platform
  [[nodiscard]] static bool IsAvailable() noexcept;
  
  // Fallback to regular read/write when zero-copy is not available
  [[nodiscard]] static ssize_t FallbackCopy(int out_fd, int in_fd, off_t offset, size_t count) noexcept;

private:
  // Platform-specific implementation
  [[nodiscard]] static ssize_t SendFileImpl(int out_fd, int in_fd, off_t* offset, size_t count) noexcept;
};

} // namespace streamit::storage
