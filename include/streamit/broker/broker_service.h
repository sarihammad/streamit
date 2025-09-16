#pragma once

#include "streamit/storage/log_dir.h"
#include "streamit/broker/idempotency_table.h"
#include "streamit/broker/broker_metrics.h"
#include "streamit/proto/streamit.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <mutex>

namespace streamit::broker {

// Broker service implementation
class BrokerServiceImpl final : public streamit::v1::Broker::Service {
public:
  // Constructor
  BrokerServiceImpl(std::shared_ptr<storage::LogDir> log_dir, 
                   std::shared_ptr<IdempotencyTable> idempotency_table);
  
  // Produce RPC implementation
  grpc::Status Produce(grpc::ServerContext* context,
                      const streamit::v1::ProduceRequest* request,
                      streamit::v1::ProduceResponse* response) override;
  
  // Fetch RPC implementation
  grpc::Status Fetch(grpc::ServerContext* context,
                    const streamit::v1::FetchRequest* request,
                    streamit::v1::FetchResponse* response) override;

private:
  std::shared_ptr<storage::LogDir> log_dir_;
  std::shared_ptr<IdempotencyTable> idempotency_table_;
  std::unique_ptr<BrokerMetrics> metrics_;
  mutable std::mutex mutex_;
  
  // Helper to validate produce request
  [[nodiscard]] grpc::Status ValidateProduceRequest(const streamit::v1::ProduceRequest* request) const;
  
  // Helper to validate fetch request
  [[nodiscard]] grpc::Status ValidateFetchRequest(const streamit::v1::FetchRequest* request) const;
  
  // Helper to convert protobuf records to storage records
  [[nodiscard]] std::vector<storage::Record> ConvertRecords(
    const google::protobuf::RepeatedPtrField<streamit::v1::Record>& proto_records) const;
  
  // Helper to convert storage records to protobuf records
  [[nodiscard]] void ConvertRecords(
    const std::vector<storage::Record>& storage_records,
    google::protobuf::RepeatedPtrField<streamit::v1::Record>* proto_records) const;
};

// Broker server management
class BrokerServer {
public:
  // Constructor
  BrokerServer(const std::string& host, uint16_t port,
               std::shared_ptr<storage::LogDir> log_dir,
               std::shared_ptr<IdempotencyTable> idempotency_table);
  
  // Start the server
  [[nodiscard]] bool Start() noexcept;
  
  // Stop the server
  [[nodiscard]] bool Stop() noexcept;
  
  // Wait for server to finish
  void Wait() noexcept;
  
  // Check if server is running
  [[nodiscard]] bool IsRunning() const noexcept;

private:
  std::string host_;
  uint16_t port_;
  std::shared_ptr<storage::LogDir> log_dir_;
  std::shared_ptr<IdempotencyTable> idempotency_table_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<BrokerServiceImpl> service_;
  std::atomic<bool> running_;
};

}

