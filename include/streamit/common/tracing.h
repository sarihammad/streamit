#pragma once

#include <iomanip>
#include <random>
#include <sstream>
#include <string>

namespace streamit::common {

// Trace context for request tracing
class TraceContext {
public:
  // Generate a new trace ID
  [[nodiscard]] static std::string GenerateTraceId() noexcept;

  // Extract trace ID from gRPC metadata
  [[nodiscard]] static std::string ExtractTraceId(const grpc::ServerContext* context) noexcept;

  // Set trace ID in gRPC metadata
  static void SetTraceId(grpc::ServerContext* context, const std::string& trace_id) noexcept;

private:
  static std::random_device rd_;
  static std::mt19937_64 gen_;
  static std::uniform_int_distribution<uint64_t> dis_;
};

// Structured logger with trace context
class StructuredLogger {
public:
  // Initialize structured logging
  static void Initialize(const std::string& level = "info");

  // Log with trace context
  template <typename... Args>
  static void LogWithTrace(const std::string& trace_id, spdlog::level::level_enum level, const std::string& message,
                           Args&&... args) {
    auto logger = spdlog::get("streamit");
    if (logger) {
      logger->log(level, "[trace_id={}] {}", trace_id, message, std::forward<Args>(args)...);
    }
  }

  // Log info with trace
  template <typename... Args>
  static void Info(const std::string& trace_id, const std::string& message, Args&&... args) {
    LogWithTrace(trace_id, spdlog::level::info, message, std::forward<Args>(args)...);
  }

  // Log error with trace
  template <typename... Args>
  static void Error(const std::string& trace_id, const std::string& message, Args&&... args) {
    LogWithTrace(trace_id, spdlog::level::err, message, std::forward<Args>(args)...);
  }

  // Log warning with trace
  template <typename... Args>
  static void Warn(const std::string& trace_id, const std::string& message, Args&&... args) {
    LogWithTrace(trace_id, spdlog::level::warn, message, std::forward<Args>(args)...);
  }

  // Log debug with trace
  template <typename... Args>
  static void Debug(const std::string& trace_id, const std::string& message, Args&&... args) {
    LogWithTrace(trace_id, spdlog::level::debug, message, std::forward<Args>(args)...);
  }
};

} // namespace streamit::common
