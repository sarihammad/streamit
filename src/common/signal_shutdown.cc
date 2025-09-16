#include "streamit/common/signal_shutdown.h"
#include <csignal>
#include <iostream>

namespace streamit::common {

std::atomic<bool> g_shutdown_requested{false};
std::function<void()> SignalHandler::shutdown_callback_;

void SignalHandler::Install() noexcept {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);
}

bool SignalHandler::IsShutdownRequested() noexcept {
  return g_shutdown_requested.load();
}

void SignalHandler::SetShutdownCallback(std::function<void()> callback) noexcept {
  shutdown_callback_ = std::move(callback);
}

void SignalHandler::Reset() noexcept {
  g_shutdown_requested.store(false);
}

void SignalHandler::HandleSignal(int signal) noexcept {
  std::cout << "Received signal " << signal << ", initiating shutdown..." << std::endl;
  g_shutdown_requested.store(true);
  
  if (shutdown_callback_) {
    shutdown_callback_();
  }
}

} // namespace streamit::common
