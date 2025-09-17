#include "streamit/common/metrics.h"
#include <iostream>
#include <mutex>

namespace streamit::common {

// Simple metrics implementation without external dependencies
class SimpleHistogram {
public:
  void Observe(double value) {
    std::lock_guard<std::mutex> lock(mutex_);
    total_ += value;
    count_++;
    if (value > max_)
      max_ = value;
    if (count_ == 1 || value < min_)
      min_ = value;
  }

  double GetSum() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return total_;
  }

  double GetCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return count_;
  }

  double GetMax() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return max_;
  }

  double GetMin() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return min_;
  }

private:
  mutable std::mutex mutex_;
  double total_ = 0.0;
  double count_ = 0.0;
  double max_ = 0.0;
  double min_ = 0.0;
};

class SimpleCounter {
public:
  void Increment(double value = 1.0) {
    std::lock_guard<std::mutex> lock(mutex_);
    value_ += value;
  }

  double GetValue() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return value_;
  }

private:
  mutable std::mutex mutex_;
  double value_ = 0.0;
};

class SimpleGauge {
public:
  void Set(double value) {
    std::lock_guard<std::mutex> lock(mutex_);
    value_ = value;
  }

  void Increment(double value = 1.0) {
    std::lock_guard<std::mutex> lock(mutex_);
    value_ += value;
  }

  double GetValue() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return value_;
  }

private:
  mutable std::mutex mutex_;
  double value_ = 0.0;
};

MetricsRegistry& MetricsRegistry::Instance() {
  static MetricsRegistry instance;
  return instance;
}

std::shared_ptr<SimpleHistogram> MetricsRegistry::CreateLatencyHistogram(
    const std::string& name, const std::string& help, const std::map<std::string, std::string>& labels) {

  std::lock_guard<std::mutex> lock(mutex_);
  auto key = name + "_" + std::to_string(std::hash<std::string>{}(help));
  if (histograms_.find(key) == histograms_.end()) {
    histograms_[key] = std::make_shared<SimpleHistogram>();
  }
  return histograms_[key];
}

std::shared_ptr<SimpleCounter> MetricsRegistry::CreateCounter(const std::string& name, const std::string& help,
                                                              const std::map<std::string, std::string>& labels) {

  std::lock_guard<std::mutex> lock(mutex_);
  auto key = name + "_" + std::to_string(std::hash<std::string>{}(help));
  if (counters_.find(key) == counters_.end()) {
    counters_[key] = std::make_shared<SimpleCounter>();
  }
  return counters_[key];
}

std::shared_ptr<SimpleGauge> MetricsRegistry::CreateGauge(const std::string& name, const std::string& help,
                                                          const std::map<std::string, std::string>& labels) {

  std::lock_guard<std::mutex> lock(mutex_);
  auto key = name + "_" + std::to_string(std::hash<std::string>{}(help));
  if (gauges_.find(key) == gauges_.end()) {
    gauges_[key] = std::make_shared<SimpleGauge>();
  }
  return gauges_[key];
}

ScopedTimer::ScopedTimer(std::shared_ptr<SimpleHistogram> histogram)
    : histogram_(histogram), start_time_(std::chrono::steady_clock::now()) {
}

ScopedTimer::~ScopedTimer() {
  if (histogram_) {
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_).count();
    histogram_->Observe(duration);
  }
}

} // namespace streamit::common
