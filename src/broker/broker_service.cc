#include "streamit/broker/broker_service.h"
#include "proto/streamit.pb.h"
#include "streamit/broker/broker_metrics.h"
#include "streamit/common/status.h"
#include "streamit/common/tracing.h"
#include <chrono>
#include <ctime>
#include <grpcpp/grpcpp.h>

namespace streamit::broker {

BrokerServiceImpl::BrokerServiceImpl(std::shared_ptr<storage::LogDir> log_dir,
                                     std::shared_ptr<IdempotencyTable> idempotency_table)
    : log_dir_(std::move(log_dir)), idempotency_table_(std::move(idempotency_table)),
      metrics_(std::make_unique<BrokerMetrics>()) {
}

grpc::Status BrokerServiceImpl::Produce(grpc::ServerContext* context, const streamit::v1::ProduceRequest* request,
                                        streamit::v1::ProduceResponse* response) {
  auto start_time = std::chrono::steady_clock::now();

  // Extract trace ID
  std::string trace_id = streamit::common::TraceContext::ExtractTraceId(context);

  // Log request
  streamit::common::StructuredLogger::Info(trace_id, "Produce request: topic={}, partition={}, records={}, ack={}",
                                           request->topic(), request->partition(), request->records().size(),
                                           (request->ack() == streamit::v1::ACK_LEADER) ? "leader" : "quorum");

  // Validate request
  auto validation_status = ValidateProduceRequest(request);
  if (!validation_status.ok()) {
    streamit::common::StructuredLogger::Error(trace_id, "Produce validation failed: {}",
                                              validation_status.error_message());
    return validation_status;
  }

  // Check idempotency if producer_id is provided
  if (!request->producer_id().empty()) {
    ProducerKey key{request->producer_id(), request->topic(), request->partition()};

    if (!idempotency_table_->IsValidSequence(key, request->sequence())) {
      response->set_error_code(streamit::v1::IDEMPOTENT_REPLAY);
      response->set_error_message("Invalid sequence number for producer");
      return grpc::Status::OK;
    }
  }

  // Convert protobuf records to storage records
  auto records = ConvertRecords(request->records());
  if (records.empty()) {
    response->set_error_code(streamit::v1::INVALID_ARGUMENT);
    response->set_error_message("No records to produce");
    return grpc::Status::OK;
  }

  // Get or create segment for the topic and partition
  auto segment_result = log_dir_->GetSegment(request->topic(), request->partition());
  if (!segment_result.ok()) {
    response->set_error_code(streamit::v1::INTERNAL);
    response->set_error_message("Failed to get segment: " + segment_result.status().message());
    return grpc::Status::OK;
  }

  auto segment = segment_result.value();

  // Append records to segment
  auto append_result = segment->Append(records);
  if (!append_result.ok()) {
    response->set_error_code(streamit::v1::INTERNAL);
    response->set_error_message("Failed to append records: " + append_result.status().message());
    return grpc::Status::OK;
  }

  int64_t base_offset = append_result.value();

  // Update idempotency table if producer_id is provided
  if (!request->producer_id().empty()) {
    ProducerKey key{request->producer_id(), request->topic(), request->partition()};
    idempotency_table_->UpdateSequence(key, request->sequence(), base_offset);
  }

  // Update high water mark
  auto hwm_result = log_dir_->SetHighWaterMark(request->topic(), request->partition(), base_offset + records.size());
  if (!hwm_result.ok()) {
    // Log warning but don't fail the request
    // In a real implementation, this would be logged
  }

  // Set response
  response->set_base_offset(base_offset);
  response->set_error_code(streamit::v1::OK);

  // Record metrics
  auto end_time = std::chrono::steady_clock::now();
  auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

  std::string ack_str = (request->ack() == streamit::v1::ACK_LEADER) ? "leader" : "quorum";
  metrics_->RecordProduceLatency(ack_str, request->topic(), request->partition(), latency_ms);

  // Calculate bytes and records
  int64_t total_bytes = 0;
  for (const auto& record : request->records()) {
    total_bytes += record.key().size() + record.value().size();
  }

  metrics_->RecordProduceBytes(request->topic(), request->partition(), total_bytes);
  metrics_->RecordProduceRecords(request->topic(), request->partition(), request->records().size());

  // Log success
  streamit::common::StructuredLogger::Info(trace_id, "Produce completed: base_offset={}, latency_ms={}", base_offset,
                                           latency_ms);

  return grpc::Status::OK;
}

grpc::Status BrokerServiceImpl::Fetch(grpc::ServerContext* context, const streamit::v1::FetchRequest* request,
                                      streamit::v1::FetchResponse* response) {
  auto start_time = std::chrono::steady_clock::now();

  // Extract trace ID
  std::string trace_id = streamit::common::TraceContext::ExtractTraceId(context);

  // Log request
  streamit::common::StructuredLogger::Info(trace_id, "Fetch request: topic={}, partition={}, offset={}, max_bytes={}",
                                           request->topic(), request->partition(), request->offset(),
                                           request->max_bytes());

  // Validate request
  auto validation_status = ValidateFetchRequest(request);
  if (!validation_status.ok()) {
    streamit::common::StructuredLogger::Error(trace_id, "Fetch validation failed: {}",
                                              validation_status.error_message());
    return validation_status;
  }

  // Get segments for the topic and partition
  auto segments_result = log_dir_->GetSegments(request->topic(), request->partition());
  if (!segments_result.ok()) {
    response->set_error_code(streamit::v1::INTERNAL);
    response->set_error_message("Failed to get segments: " + segments_result.status().message());
    return grpc::Status::OK;
  }

  const auto& segments = segments_result.value();
  if (segments.empty()) {
    response->set_high_watermark(0);
    response->set_error_code(streamit::v1::OK);
    return grpc::Status::OK;
  }

  // Find the segment containing the requested offset
  std::shared_ptr<storage::Segment> target_segment = nullptr;
  for (const auto& segment : segments) {
    if (request->offset() >= segment->BaseOffset() && request->offset() < segment->EndOffset()) {
      target_segment = segment;
      break;
    }
  }

  if (!target_segment) {
    // Offset is beyond the end of all segments
    auto end_offset_result = log_dir_->GetEndOffset(request->topic(), request->partition());
    if (end_offset_result.ok()) {
      response->set_high_watermark(end_offset_result.value());
    } else {
      response->set_high_watermark(0);
    }
    response->set_error_code(streamit::v1::OFFSET_OUT_OF_RANGE);
    response->set_error_message("Requested offset is beyond the end of all segments");
    return grpc::Status::OK;
  }

  // Read batches from the segment
  auto batches_result = target_segment->Read(request->offset(), request->max_bytes());
  if (!batches_result.ok()) {
    response->set_error_code(streamit::v1::INTERNAL);
    response->set_error_message("Failed to read from segment: " + batches_result.status().message());
    return grpc::Status::OK;
  }

  const auto& batches = batches_result.value();

  // Convert batches to protobuf format
  for (const auto& batch : batches) {
    auto* proto_batch = response->add_batches();
    proto_batch->set_base_offset(batch.base_offset);
    proto_batch->set_crc32(batch.crc32);

    // Convert records
    for (const auto& record : batch.records) {
      auto* proto_record = proto_batch->add_records();
      proto_record->set_key(record.key);
      proto_record->set_value(record.value);
      proto_record->set_timestamp_ms(record.timestamp_ms);
    }
  }

  // Set high water mark
  auto hwm_result = log_dir_->GetHighWaterMark(request->topic(), request->partition());
  if (hwm_result.ok()) {
    response->set_high_watermark(hwm_result.value());
  } else {
    response->set_high_watermark(0);
  }

  response->set_error_code(streamit::v1::OK);

  // Record metrics
  auto end_time = std::chrono::steady_clock::now();
  auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

  metrics_->RecordFetchLatency(request->topic(), request->partition(), latency_ms);

  // Calculate bytes fetched
  int64_t total_bytes = 0;
  for (const auto& batch : response->batches()) {
    total_bytes += batch.payload().size();
  }

  metrics_->RecordFetchBytes(request->topic(), request->partition(), total_bytes);

  // Log success
  streamit::common::StructuredLogger::Info(trace_id, "Fetch completed: batches={}, bytes={}, latency_ms={}",
                                           response->batches().size(), total_bytes, latency_ms);

  return grpc::Status::OK;
}

grpc::Status BrokerServiceImpl::ValidateProduceRequest(const streamit::v1::ProduceRequest* request) const {
  if (request->topic().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Topic cannot be empty");
  }

  if (request->partition() < 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Partition must be non-negative");
  }

  if (request->records().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Records cannot be empty");
  }

  return grpc::Status::OK;
}

grpc::Status BrokerServiceImpl::ValidateFetchRequest(const streamit::v1::FetchRequest* request) const {
  if (request->topic().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Topic cannot be empty");
  }

  if (request->partition() < 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Partition must be non-negative");
  }

  if (request->offset() < 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Offset must be non-negative");
  }

  if (request->max_bytes() <= 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Max bytes must be positive");
  }

  return grpc::Status::OK;
}

std::vector<storage::Record> BrokerServiceImpl::ConvertRecords(
    const google::protobuf::RepeatedPtrField<streamit::v1::Record>& proto_records) const {
  std::vector<storage::Record> records;
  records.reserve(proto_records.size());

  for (const auto& proto_record : proto_records) {
    int64_t timestamp_ms = proto_record.timestamp_ms();
    if (timestamp_ms == 0) {
      // Use current timestamp if not provided
      timestamp_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
              .count();
    }

    records.emplace_back(proto_record.key(), proto_record.value(), timestamp_ms);
  }

  return records;
}

void BrokerServiceImpl::ConvertRecords(const std::vector<storage::Record>& storage_records,
                                       google::protobuf::RepeatedPtrField<streamit::v1::Record>* proto_records) const {
  for (const auto& record : storage_records) {
    auto* proto_record = proto_records->Add();
    proto_record->set_key(record.key);
    proto_record->set_value(record.value);
    proto_record->set_timestamp_ms(record.timestamp_ms);
  }
}

BrokerServer::BrokerServer(const std::string& host, uint16_t port, std::shared_ptr<storage::LogDir> log_dir,
                           std::shared_ptr<IdempotencyTable> idempotency_table)
    : host_(host), port_(port), log_dir_(std::move(log_dir)), idempotency_table_(std::move(idempotency_table)),
      running_(false) {
}

bool BrokerServer::Start() noexcept {
  try {
    service_ = std::make_unique<BrokerServiceImpl>(log_dir_, idempotency_table_);

    grpc::ServerBuilder builder;
    std::string server_address = host_ + ":" + std::to_string(port_);

    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service_.get());

    server_ = builder.BuildAndStart();
    if (!server_) {
      return false;
    }

    running_.store(true);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool BrokerServer::Stop() noexcept {
  if (server_) {
    server_->Shutdown();
    running_.store(false);
    return true;
  }
  return false;
}

void BrokerServer::Wait() noexcept {
  if (server_) {
    server_->Wait();
  }
}

bool BrokerServer::IsRunning() const noexcept {
  return running_.load();
}

} // namespace streamit::broker
