#include "streamit/common/tracing.h"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <grpcpp/grpcpp.h>
#include <random>

namespace streamit::common {

std::random_device TraceContext::rd_;
std::mt19937_64 TraceContext::gen_(rd_());
std::uniform_int_distribution<uint64_t> TraceContext::dis_;

std::string TraceContext::GenerateTraceId() noexcept {
  uint64_t value = dis_(gen_);
  
  std::ostringstream oss;
  oss << std::hex << std::setfill('0') << std::setw(16) << value;
  return oss.str();
}

std::string TraceContext::ExtractTraceId(const grpc::ServerContext* context) noexcept {
  if (!context) {
    return GenerateTraceId();
  }
  
  auto metadata = context->client_metadata();
  auto it = metadata.find("x-trace-id");
  if (it != metadata.end()) {
    return std::string(it->second.data(), it->second.length());
  }
  
  return GenerateTraceId();
}

void TraceContext::SetTraceId(grpc::ServerContext* context, const std::string& trace_id) noexcept {
  if (context) {
    context->AddInitialMetadata("x-trace-id", trace_id);
  }
}

void StructuredLogger::Initialize(const std::string& level) {
  // Create console sink
  auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  console_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");
  
  // Create file sink for structured logs
  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
    "logs/streamit.json", 1024 * 1024 * 10, 5); // 10MB, 5 files
  file_sink->set_pattern(R"({"timestamp":"%Y-%m-%dT%H:%M:%S.%eZ","level":"%l","thread":"%t","message":"%v"})");
  
  // Create logger with both sinks
  auto logger = std::make_shared<spdlog::logger>("streamit", 
    spdlog::sinks_init_list{console_sink, file_sink});
  
  // Set log level
  if (level == "debug") {
    logger->set_level(spdlog::level::debug);
  } else if (level == "info") {
    logger->set_level(spdlog::level::info);
  } else if (level == "warn") {
    logger->set_level(spdlog::level::warn);
  } else if (level == "error") {
    logger->set_level(spdlog::level::err);
  } else {
    logger->set_level(spdlog::level::info);
  }
  
  spdlog::register_logger(logger);
  spdlog::set_default_logger(logger);
}

} // namespace streamit::common
