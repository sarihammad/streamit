#include "consumer.h"
#include "streamit/proto/streamit.grpc.pb.h"
#include <chrono>
#include <ctime>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <thread>

namespace streamit::cli {

int RunConsumer(int argc, char* argv[]) {
  // Parse command line arguments
  std::string broker_host = "localhost";
  int broker_port = 9092;
  std::string coordinator_host = "localhost";
  int coordinator_port = 9094;
  std::string topic;
  std::string group = "default-group";
  int64_t from_offset = 0;
  int max_bytes = 1024 * 1024; // 1MB
  bool follow = false;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintConsumerHelp();
      return 0;
    } else if (arg == "--broker" && i + 1 < argc) {
      broker_host = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      broker_port = std::stoi(argv[++i]);
    } else if (arg == "--coordinator" && i + 1 < argc) {
      coordinator_host = argv[++i];
    } else if (arg == "--coordinator-port" && i + 1 < argc) {
      coordinator_port = std::stoi(argv[++i]);
    } else if (arg == "--topic" && i + 1 < argc) {
      topic = argv[++i];
    } else if (arg == "--group" && i + 1 < argc) {
      group = argv[++i];
    } else if (arg == "--from" && i + 1 < argc) {
      from_offset = std::stoll(argv[++i]);
    } else if (arg == "--max-bytes" && i + 1 < argc) {
      max_bytes = std::stoi(argv[++i]);
    } else if (arg == "--follow" || arg == "-f") {
      follow = true;
    }
  }

  if (topic.empty()) {
    std::cerr << "Error: --topic is required" << std::endl;
    PrintConsumerHelp();
    return 1;
  }

  // Create gRPC channels
  std::string broker_address = broker_host + ":" + std::to_string(broker_port);
  auto broker_channel = grpc::CreateChannel(broker_address, grpc::InsecureChannelCredentials());
  auto broker_stub = streamit::v1::Broker::NewStub(broker_channel);

  std::string coordinator_address = coordinator_host + ":" + std::to_string(coordinator_port);
  auto coordinator_channel = grpc::CreateChannel(coordinator_address, grpc::InsecureChannelCredentials());
  auto coordinator_stub = streamit::v1::Coordinator::NewStub(coordinator_channel);

  // Generate member ID
  std::string member_id = "consumer-" + std::to_string(std::time(nullptr));

  // Join consumer group
  streamit::v1::PollAssignmentRequest assignment_request;
  assignment_request.set_group(group);
  assignment_request.set_member_id(member_id);
  assignment_request.add_topics(topic);

  streamit::v1::PollAssignmentResponse assignment_response;
  grpc::ClientContext assignment_context;
  grpc::Status assignment_status =
      coordinator_stub->PollAssignment(&assignment_context, assignment_request, &assignment_response);

  if (!assignment_status.ok()) {
    std::cerr << "Failed to join consumer group: " << assignment_status.error_message() << std::endl;
    return 1;
  }

  std::cout << "Joined consumer group '" << group << "' as member '" << member_id << "'" << std::endl;
  std::cout << "Assigned partitions: ";
  for (const auto& assignment : assignment_response.assignments()) {
    for (int partition : assignment.partitions()) {
      std::cout << topic << ":" << partition << " ";
    }
  }
  std::cout << std::endl;

  // Consumer loop
  int64_t current_offset = from_offset;
  int64_t total_messages = 0;
  int64_t total_bytes = 0;

  std::cout << "Starting consumer for topic '" << topic << "' from offset " << from_offset << "..." << std::endl;

  do {
    // Fetch messages
    streamit::v1::FetchRequest fetch_request;
    fetch_request.set_topic(topic);
    fetch_request.set_partition(0); // Simplified - would iterate over assigned partitions
    fetch_request.set_offset(current_offset);
    fetch_request.set_max_bytes(max_bytes);

    streamit::v1::FetchResponse fetch_response;
    grpc::ClientContext fetch_context;
    grpc::Status fetch_status = broker_stub->Fetch(&fetch_context, fetch_request, &fetch_response);

    if (!fetch_status.ok()) {
      std::cerr << "Fetch failed: " << fetch_status.error_message() << std::endl;
      break;
    }

    // Process messages
    for (const auto& batch : fetch_response.batches()) {
      for (const auto& record : batch.records()) {
        std::cout << "[" << record.timestamp_ms() << "] "
                  << "key=" << record.key() << " value=" << record.value().substr(0, 50)
                  << (record.value().size() > 50 ? "..." : "") << std::endl;

        total_messages++;
        total_bytes += record.value().size();
        current_offset++;
      }
    }

    // Commit offset
    if (total_messages > 0) {
      streamit::v1::CommitOffsetRequest commit_request;
      commit_request.set_group(group);
      commit_request.set_topic(topic);
      commit_request.set_partition(0);
      commit_request.set_offset(current_offset);

      streamit::v1::CommitOffsetResponse commit_response;
      grpc::ClientContext commit_context;
      grpc::Status commit_status = coordinator_stub->CommitOffset(&commit_context, commit_request, &commit_response);

      if (!commit_status.ok()) {
        std::cerr << "Failed to commit offset: " << commit_status.error_message() << std::endl;
      }
    }

    // If not following, break after first fetch
    if (!follow) {
      break;
    }

    // Sleep before next fetch
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

  } while (follow);

  std::cout << "\nConsumer completed:" << std::endl;
  std::cout << "  Messages: " << total_messages << std::endl;
  std::cout << "  Bytes: " << total_bytes << std::endl;
  std::cout << "  Last offset: " << current_offset << std::endl;

  return 0;
}

void PrintConsumerHelp() {
  std::cout << "Usage: streamit_cli consume [options]\n"
            << "\n"
            << "Options:\n"
            << "  --broker HOST           Broker hostname (default: localhost)\n"
            << "  --port PORT             Broker port (default: 9092)\n"
            << "  --coordinator HOST      Coordinator hostname (default: localhost)\n"
            << "  --coordinator-port PORT Coordinator port (default: 9094)\n"
            << "  --topic TOPIC           Topic name (required)\n"
            << "  --group GROUP           Consumer group (default: default-group)\n"
            << "  --from OFFSET           Starting offset (default: 0)\n"
            << "  --max-bytes BYTES       Maximum bytes per fetch (default: 1MB)\n"
            << "  --follow, -f            Follow new messages\n"
            << "  --help, -h              Show this help message\n";
}

} // namespace streamit::cli
