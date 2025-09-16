#include "producer.h"
#include "streamit/proto/streamit.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <chrono>
#include <random>
#include <thread>
#include <ctime>

namespace streamit::cli {

int RunProducer(int argc, char* argv[]) {
  // Parse command line arguments
  std::string broker_host = "localhost";
  int broker_port = 9092;
  std::string topic;
  int partition = 0;
  int rate = 1000;
  int size = 1024;
  std::string acks = "leader";
  int duration_seconds = 10;
  std::string producer_id;
  
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintProducerHelp();
      return 0;
    } else if (arg == "--broker" && i + 1 < argc) {
      broker_host = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      broker_port = std::stoi(argv[++i]);
    } else if (arg == "--topic" && i + 1 < argc) {
      topic = argv[++i];
    } else if (arg == "--partition" && i + 1 < argc) {
      partition = std::stoi(argv[++i]);
    } else if (arg == "--rate" && i + 1 < argc) {
      rate = std::stoi(argv[++i]);
    } else if (arg == "--size" && i + 1 < argc) {
      size = std::stoi(argv[++i]);
    } else if (arg == "--acks" && i + 1 < argc) {
      acks = argv[++i];
    } else if (arg == "--duration" && i + 1 < argc) {
      duration_seconds = std::stoi(argv[++i]);
    } else if (arg == "--producer-id" && i + 1 < argc) {
      producer_id = argv[++i];
    }
  }
  
  if (topic.empty()) {
    std::cerr << "Error: --topic is required" << std::endl;
    PrintProducerHelp();
    return 1;
  }
  
  // Generate producer ID if not provided
  if (producer_id.empty()) {
    producer_id = "producer-" + std::to_string(std::time(nullptr));
  }
  
  // Create gRPC channel
  std::string server_address = broker_host + ":" + std::to_string(broker_port);
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  auto stub = streamit::v1::Broker::NewStub(channel);
  
  // Convert acks string to enum
  streamit::v1::Ack ack_level = streamit::v1::ACK_LEADER;
  if (acks == "quorum") {
    ack_level = streamit::v1::ACK_QUORUM;
  }
  
  // Generate random data
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);
  
  // Producer loop
  auto start_time = std::chrono::steady_clock::now();
  auto end_time = start_time + std::chrono::seconds(duration_seconds);
  int64_t sequence = 0;
  int64_t total_messages = 0;
  int64_t total_bytes = 0;
  
  std::cout << "Starting producer for topic '" << topic << "' partition " << partition 
            << " at " << rate << " msg/s for " << duration_seconds << " seconds..." << std::endl;
  
  while (std::chrono::steady_clock::now() < end_time) {
    auto batch_start = std::chrono::steady_clock::now();
    
    // Create request
    streamit::v1::ProduceRequest request;
    request.set_topic(topic);
    request.set_partition(partition);
    request.set_ack(ack_level);
    request.set_producer_id(producer_id);
    request.set_sequence(sequence++);
    
    // Generate random record
    std::string key = "key-" + std::to_string(sequence);
    std::string value(size, 'x');
    for (int i = 0; i < size; ++i) {
      value[i] = static_cast<char>(dis(gen));
    }
    
    auto* record = request.add_records();
    record->set_key(key);
    record->set_value(value);
    record->set_timestamp_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count());
    
    // Send request
    streamit::v1::ProduceResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub->Produce(&context, request, &response);
    
    if (status.ok()) {
      total_messages++;
      total_bytes += size;
    } else {
      std::cerr << "Produce failed: " << status.error_message() << std::endl;
    }
    
    // Rate limiting
    auto batch_end = std::chrono::steady_clock::now();
    auto batch_duration = std::chrono::duration_cast<std::chrono::microseconds>(
      batch_end - batch_start).count();
    
    int64_t target_duration = 1000000 / rate; // microseconds per message
    if (batch_duration < target_duration) {
      std::this_thread::sleep_for(std::chrono::microseconds(target_duration - batch_duration));
    }
  }
  
  auto actual_duration = std::chrono::duration_cast<std::chrono::seconds>(
    std::chrono::steady_clock::now() - start_time).count();
  
  double actual_rate = static_cast<double>(total_messages) / actual_duration;
  double throughput_mbps = (static_cast<double>(total_bytes) / (1024 * 1024)) / actual_duration;
  
  std::cout << "\nProducer completed:" << std::endl;
  std::cout << "  Messages: " << total_messages << std::endl;
  std::cout << "  Bytes: " << total_bytes << std::endl;
  std::cout << "  Duration: " << actual_duration << " seconds" << std::endl;
  std::cout << "  Rate: " << actual_rate << " msg/s" << std::endl;
  std::cout << "  Throughput: " << throughput_mbps << " MB/s" << std::endl;
  
  return 0;
}

void PrintProducerHelp() {
  std::cout << "Usage: streamit_cli produce [options]\n"
            << "\n"
            << "Options:\n"
            << "  --broker HOST        Broker hostname (default: localhost)\n"
            << "  --port PORT          Broker port (default: 9092)\n"
            << "  --topic TOPIC        Topic name (required)\n"
            << "  --partition PART     Partition number (default: 0)\n"
            << "  --rate RATE          Messages per second (default: 1000)\n"
            << "  --size SIZE          Message size in bytes (default: 1024)\n"
            << "  --acks ACKS          Acknowledgment level: leader|quorum (default: leader)\n"
            << "  --duration SECONDS   Duration in seconds (default: 10)\n"
            << "  --producer-id ID     Producer ID (default: auto-generated)\n"
            << "  --help, -h           Show this help message\n";
}

}
