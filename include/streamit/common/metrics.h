#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace streamit::common {

// Forward declarations for simple metrics classes
class SimpleHistogram;
class SimpleCounter;
class SimpleGauge;

// Simple metrics registry without external dependencies
class MetricsRegistry {
public:
  // Get singleton instance
  static MetricsRegistry& Instance();

  // Create a histogram with standard latency buckets
  [[nodiscard]] std::shared_ptr<SimpleHistogram> CreateLatencyHistogram(
      const std::string& name, const std::string& help, const std::map<std::string, std::string>& labels = {});

  // Create a counter
  [[nodiscard]] std::shared_ptr<SimpleCounter> CreateCounter(const std::string& name, const std::string& help,
                                                             const std::map<std::string, std::string>& labels = {});

  // Create a gauge
  [[nodiscard]] std::shared_ptr<SimpleGauge> CreateGauge(const std::string& name, const std::string& help,
                                                         const std::map<std::string, std::string>& labels = {});

private:
  MetricsRegistry() = default;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<SimpleHistogram>> histograms_;
  std::unordered_map<std::string, std::shared_ptr<SimpleCounter>> counters_;
  std::unordered_map<std::string, std::shared_ptr<SimpleGauge>> gauges_;
};

// RAII timer for measuring latency
class ScopedTimer {
public:
  ScopedTimer(std::shared_ptr<SimpleHistogram> histogram);
  ~ScopedTimer();

private:
  std::shared_ptr<SimpleHistogram> histogram_;
  std::chrono::steady_clock::time_point start_time_;
};

// Convenience macros for metrics
#define STREAMIT_METRICS_LATENCY_HISTOGRAM(name, help, labels)                                                         \
  streamit::common::MetricsRegistry::Instance().CreateLatencyHistogram(name, help, labels)

#define STREAMIT_METRICS_COUNTER(name, help, labels)                                                                   \
  streamit::common::MetricsRegistry::Instance().CreateCounter(name, help, labels)

#define STREAMIT_METRICS_GAUGE(name, help, labels)                                                                     \
  streamit::common::MetricsRegistry::Instance().CreateGauge(name, help, labels)

#define STREAMIT_METRICS_TIMER(histogram) streamit::common::ScopedTimer timer(histogram)

} // namespace streamit::common
