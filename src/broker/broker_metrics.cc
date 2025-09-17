#include "streamit/broker/broker_metrics.h"

namespace streamit::broker {

BrokerMetrics::BrokerMetrics() {
  // Initialize produce metrics
  produce_latency_hist_ =
      STREAMIT_METRICS_LATENCY_HISTOGRAM("streamit_produce_latency_ms", "Produce request latency in milliseconds", {});

  produce_bytes_counter_ = STREAMIT_METRICS_COUNTER("streamit_bytes_in_total", "Total bytes produced", {});

  produce_records_counter_ = STREAMIT_METRICS_COUNTER("streamit_records_in_total", "Total records produced", {});

  // Initialize fetch metrics
  fetch_latency_hist_ =
      STREAMIT_METRICS_LATENCY_HISTOGRAM("streamit_fetch_latency_ms", "Fetch request latency in milliseconds", {});

  fetch_bytes_counter_ = STREAMIT_METRICS_COUNTER("streamit_bytes_out_total", "Total bytes fetched", {});

  // Initialize storage metrics
  segment_rolls_counter_ = STREAMIT_METRICS_COUNTER("streamit_segment_rolls_total", "Total segment rolls", {});

  crc_mismatches_counter_ = STREAMIT_METRICS_COUNTER("streamit_crc_mismatches_total", "Total CRC mismatches", {});

  // Initialize high water mark metrics
  high_watermark_gauge_ = STREAMIT_METRICS_GAUGE("streamit_high_watermark", "High water mark offset", {});

  // Initialize replication lag metrics
  replication_lag_gauge_ = STREAMIT_METRICS_GAUGE("streamit_replication_lag", "Replication lag in offsets", {});
}

void BrokerMetrics::RecordProduceLatency(const std::string& ack, const std::string& topic, int32_t partition,
                                         double latency_ms) noexcept {
  auto labels = CreateLabels(topic, partition);
  labels["ack"] = ack;

  // Create a new histogram instance for this specific combination
  auto hist = STREAMIT_METRICS_LATENCY_HISTOGRAM("streamit_produce_latency_ms",
                                                 "Produce request latency in milliseconds", labels);
  hist->Observe(latency_ms);
}

void BrokerMetrics::RecordProduceBytes(const std::string& topic, int32_t partition, int64_t bytes) noexcept {
  auto labels = CreateLabels(topic, partition);
  auto counter = STREAMIT_METRICS_COUNTER("streamit_bytes_in_total", "Total bytes produced", labels);
  counter->Increment(bytes);
}

void BrokerMetrics::RecordProduceRecords(const std::string& topic, int32_t partition, int64_t records) noexcept {
  auto labels = CreateLabels(topic, partition);
  auto counter = STREAMIT_METRICS_COUNTER("streamit_records_in_total", "Total records produced", labels);
  counter->Increment(records);
}

void BrokerMetrics::RecordFetchLatency(const std::string& topic, int32_t partition, double latency_ms) noexcept {
  auto labels = CreateLabels(topic, partition);
  auto hist =
      STREAMIT_METRICS_LATENCY_HISTOGRAM("streamit_fetch_latency_ms", "Fetch request latency in milliseconds", labels);
  hist->Observe(latency_ms);
}

void BrokerMetrics::RecordFetchBytes(const std::string& topic, int32_t partition, int64_t bytes) noexcept {
  auto labels = CreateLabels(topic, partition);
  auto counter = STREAMIT_METRICS_COUNTER("streamit_bytes_out_total", "Total bytes fetched", labels);
  counter->Increment(bytes);
}

void BrokerMetrics::RecordSegmentRoll(const std::string& topic, int32_t partition) noexcept {
  auto labels = CreateLabels(topic, partition);
  auto counter = STREAMIT_METRICS_COUNTER("streamit_segment_rolls_total", "Total segment rolls", labels);
  counter->Increment();
}

void BrokerMetrics::RecordCrcMismatch(const std::string& topic, int32_t partition) noexcept {
  auto labels = CreateLabels(topic, partition);
  auto counter = STREAMIT_METRICS_COUNTER("streamit_crc_mismatches_total", "Total CRC mismatches", labels);
  counter->Increment();
}

void BrokerMetrics::SetHighWaterMark(const std::string& topic, int32_t partition, int64_t offset) noexcept {
  auto labels = CreateLabels(topic, partition);
  auto gauge = STREAMIT_METRICS_GAUGE("streamit_high_watermark", "High water mark offset", labels);
  gauge->Set(offset);
}

void BrokerMetrics::SetReplicationLag(const std::string& topic, int32_t partition, int64_t lag) noexcept {
  auto labels = CreateLabels(topic, partition);
  auto gauge = STREAMIT_METRICS_GAUGE("streamit_replication_lag", "Replication lag in offsets", labels);
  gauge->Set(lag);
}

std::map<std::string, std::string> BrokerMetrics::CreateLabels(const std::string& topic,
                                                               int32_t partition) const noexcept {
  return {{"topic", topic}, {"partition", std::to_string(partition)}};
}

} // namespace streamit::broker
