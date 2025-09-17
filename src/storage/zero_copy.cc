#include "streamit/storage/zero_copy.h"
#include <cstring>
#include <sys/sendfile.h>
#include <unistd.h>

namespace streamit::storage {

ssize_t ZeroCopy::SendFile(int out_fd, int in_fd, off_t* offset, size_t count) noexcept {
  if (!IsAvailable()) {
    return FallbackCopy(out_fd, in_fd, *offset, count);
  }

  return SendFileImpl(out_fd, in_fd, offset, count);
}

bool ZeroCopy::IsAvailable() noexcept {
#ifdef __linux__
  return true;
#else
  return false;
#endif
}

ssize_t ZeroCopy::FallbackCopy(int out_fd, int in_fd, off_t offset, size_t count) noexcept {
  // Fallback to regular read/write
  const size_t buffer_size = 64 * 1024; // 64KB buffer
  char buffer[buffer_size];

  ssize_t total_bytes = 0;
  size_t remaining = count;

  while (remaining > 0) {
    size_t to_read = std::min(remaining, buffer_size);
    ssize_t bytes_read = pread(in_fd, buffer, to_read, offset + total_bytes);

    if (bytes_read <= 0) {
      return bytes_read;
    }

    ssize_t bytes_written = write(out_fd, buffer, bytes_read);
    if (bytes_written != bytes_read) {
      return -1;
    }

    total_bytes += bytes_written;
    remaining -= bytes_written;
  }

  return total_bytes;
}

ssize_t ZeroCopy::SendFileImpl(int out_fd, int in_fd, off_t* offset, size_t count) noexcept {
#ifdef __linux__
  return ::sendfile(out_fd, in_fd, offset, count);
#else
  return FallbackCopy(out_fd, in_fd, *offset, count);
#endif
}

} // namespace streamit::storage
