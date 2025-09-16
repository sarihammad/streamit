#pragma once

#include <string>
#include <memory>
#include <chrono>
#include <map>

namespace streamit::common {

// Prometheus metrics registry and helpers
class MetricsRegistry {
public:
  // Get singleton instance
  static MetricsRegistry& Instance();
  
  // Get the Prometheus registry
  [[nodiscard]] prometheus::Registry& GetRegistry() noexcept;
  
  // Create a histogram with standard latency buckets
  [[nodiscard]] std::shared_ptr<prometheus::Histogram> CreateLatencyHistogram(
    const std::string& name, const std::string& help, 
    const std::map<std::string, std::string>& labels = {});
  
  // Create a counter
  [[nodiscard]] std::shared_ptr<prometheus::Counter> CreateCounter(
    const std::string& name, const std::string& help,
    const std::map<std::string, std::string>& labels = {});
  
  // Create a gauge
  [[nodiscard]] std::shared_ptr<prometheus::Gauge> CreateGauge(
    const std::string& name, const std::string& help,
    const std::map<std::string, std::string>& labels = {});

private:
  MetricsRegistry() = default;
  std::unique_ptr<prometheus::Registry> registry_;
};

// RAII timer for measuring latency
class ScopedTimer {
public:
  ScopedTimer(std::shared_ptr<prometheus::Histogram> histogram);
  ~ScopedTimer();

private:
  std::shared_ptr<prometheus::Histogram> histogram_;
  std::chrono::steady_clock::time_point start_time_;
};

// Convenience macros for metrics
#define STREAMIT_METRICS_LATENCY_HISTOGRAM(name, help, labels) \
  streamit::common::MetricsRegistry::Instance().CreateLatencyHistogram(name, help, labels)

#define STREAMIT_METRICS_COUNTER(name, help, labels) \
  streamit::common::MetricsRegistry::Instance().CreateCounter(name, help, labels)

#define STREAMIT_METRICS_GAUGE(name, help, labels) \
  streamit::common::MetricsRegistry::Instance().CreateGauge(name, help, labels)

#define STREAMIT_METRICS_TIMER(histogram) \
  streamit::common::ScopedTimer timer(histogram)

} // namespace streamit::common
