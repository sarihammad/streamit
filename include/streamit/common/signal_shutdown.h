#pragma once

#include <atomic>
#include <csignal>
#include <functional>

namespace streamit::common {

// Global shutdown flag
extern std::atomic<bool> g_shutdown_requested;

// Signal handler for clean shutdown
class SignalHandler {
public:
  // Install signal handlers for SIGINT and SIGTERM
  static void Install() noexcept;

  // Check if shutdown has been requested
  [[nodiscard]] static bool IsShutdownRequested() noexcept;

  // Set a custom shutdown callback
  static void SetShutdownCallback(std::function<void()> callback) noexcept;

  // Reset shutdown flag (for testing)
  static void Reset() noexcept;

private:
  static std::function<void()> shutdown_callback_;

  // Signal handler function
  static void HandleSignal(int signal) noexcept;
};

} // namespace streamit::common
