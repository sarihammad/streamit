#include "streamit/common/metrics.h"
#include <prometheus/registry.h>
#include <prometheus/histogram.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>

namespace streamit::common {

MetricsRegistry& MetricsRegistry::Instance() {
  static MetricsRegistry instance;
  return instance;
}

prometheus::Registry& MetricsRegistry::GetRegistry() noexcept {
  if (!registry_) {
    registry_ = std::make_unique<prometheus::Registry>();
  }
  return *registry_;
}

std::shared_ptr<prometheus::Histogram> MetricsRegistry::CreateLatencyHistogram(
    const std::string& name, const std::string& help, 
    const std::map<std::string, std::string>& labels) {
  
  auto& registry = GetRegistry();
  
  // Standard latency buckets in milliseconds
  prometheus::Histogram::BucketBoundaries buckets{
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096
  };
  
  auto& histogram_family = prometheus::BuildHistogram()
    .Name(name)
    .Help(help)
    .Register(registry);
  
  return std::make_shared<prometheus::Histogram>(
    histogram_family.Add(labels, buckets));
}

std::shared_ptr<prometheus::Counter> MetricsRegistry::CreateCounter(
    const std::string& name, const std::string& help,
    const std::map<std::string, std::string>& labels) {
  
  auto& registry = GetRegistry();
  
  auto& counter_family = prometheus::BuildCounter()
    .Name(name)
    .Help(help)
    .Register(registry);
  
  return std::make_shared<prometheus::Counter>(
    counter_family.Add(labels));
}

std::shared_ptr<prometheus::Gauge> MetricsRegistry::CreateGauge(
    const std::string& name, const std::string& help,
    const std::map<std::string, std::string>& labels) {
  
  auto& registry = GetRegistry();
  
  auto& gauge_family = prometheus::BuildGauge()
    .Name(name)
    .Help(help)
    .Register(registry);
  
  return std::make_shared<prometheus::Gauge>(
    gauge_family.Add(labels));
}

ScopedTimer::ScopedTimer(std::shared_ptr<prometheus::Histogram> histogram)
  : histogram_(histogram)
  , start_time_(std::chrono::steady_clock::now()) {}

ScopedTimer::~ScopedTimer() {
  if (histogram_) {
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time_).count();
    histogram_->Observe(duration);
  }
}

} // namespace streamit::common
