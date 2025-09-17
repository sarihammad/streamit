#pragma once

#include "streamit/common/metrics.h"
#include <map>
#include <memory>
#include <string>

namespace streamit::broker {

// Broker-specific metrics
class BrokerMetrics {
public:
  // Constructor
  BrokerMetrics();

  // Produce metrics
  void RecordProduceLatency(const std::string& ack, const std::string& topic, int32_t partition,
                            double latency_ms) noexcept;
  void RecordProduceBytes(const std::string& topic, int32_t partition, int64_t bytes) noexcept;
  void RecordProduceRecords(const std::string& topic, int32_t partition, int64_t records) noexcept;

  // Fetch metrics
  void RecordFetchLatency(const std::string& topic, int32_t partition, double latency_ms) noexcept;
  void RecordFetchBytes(const std::string& topic, int32_t partition, int64_t bytes) noexcept;

  // Storage metrics
  void RecordSegmentRoll(const std::string& topic, int32_t partition) noexcept;
  void RecordCrcMismatch(const std::string& topic, int32_t partition) noexcept;

  // High water mark metrics
  void SetHighWaterMark(const std::string& topic, int32_t partition, int64_t offset) noexcept;

  // Replication lag metrics (for future use)
  void SetReplicationLag(const std::string& topic, int32_t partition, int64_t lag) noexcept;

private:
  // Produce metrics
  std::shared_ptr<streamit::common::SimpleHistogram> produce_latency_hist_;
  std::shared_ptr<streamit::common::SimpleCounter> produce_bytes_counter_;
  std::shared_ptr<streamit::common::SimpleCounter> produce_records_counter_;

  // Fetch metrics
  std::shared_ptr<streamit::common::SimpleHistogram> fetch_latency_hist_;
  std::shared_ptr<streamit::common::SimpleCounter> fetch_bytes_counter_;

  // Storage metrics
  std::shared_ptr<streamit::common::SimpleCounter> segment_rolls_counter_;
  std::shared_ptr<streamit::common::SimpleCounter> crc_mismatches_counter_;

  // High water mark metrics
  std::shared_ptr<streamit::common::SimpleGauge> high_watermark_gauge_;

  // Replication lag metrics
  std::shared_ptr<streamit::common::SimpleGauge> replication_lag_gauge_;

  // Helper to create labels
  [[nodiscard]] std::map<std::string, std::string> CreateLabels(const std::string& topic,
                                                                int32_t partition) const noexcept;
};

} // namespace streamit::broker
