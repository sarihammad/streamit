#include "admin.h"
#include "streamit/proto/streamit.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>

namespace streamit::cli {

int RunAdmin(int argc, char* argv[]) {
  if (argc < 2) {
    PrintAdminHelp();
    return 1;
  }
  
  std::string command = argv[1];
  
  if (command == "create-topic") {
    return RunCreateTopic(argc - 1, argv + 1);
  } else if (command == "describe-topic") {
    return RunDescribeTopic(argc - 1, argv + 1);
  } else if (command == "list-topics") {
    return RunListTopics(argc - 1, argv + 1);
  } else {
    std::cerr << "Unknown admin command: " << command << std::endl;
    PrintAdminHelp();
    return 1;
  }
}

int RunCreateTopic(int argc, char* argv[]) {
  // Parse command line arguments
  std::string controller_host = "localhost";
  int controller_port = 9093;
  std::string topic;
  int partitions = 1;
  int replication_factor = 1;
  
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintCreateTopicHelp();
      return 0;
    } else if (arg == "--controller" && i + 1 < argc) {
      controller_host = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      controller_port = std::stoi(argv[++i]);
    } else if (arg == "--topic" && i + 1 < argc) {
      topic = argv[++i];
    } else if (arg == "--partitions" && i + 1 < argc) {
      partitions = std::stoi(argv[++i]);
    } else if (arg == "--replication-factor" && i + 1 < argc) {
      replication_factor = std::stoi(argv[++i]);
    }
  }
  
  if (topic.empty()) {
    std::cerr << "Error: --topic is required" << std::endl;
    PrintCreateTopicHelp();
    return 1;
  }
  
  // Create gRPC channel
  std::string server_address = controller_host + ":" + std::to_string(controller_port);
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  auto stub = streamit::v1::Controller::NewStub(channel);
  
  // Create topic request
  streamit::v1::CreateTopicRequest request;
  request.set_topic(topic);
  request.set_partitions(partitions);
  request.set_replication_factor(replication_factor);
  
  streamit::v1::CreateTopicResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub->CreateTopic(&context, request, &response);
  
  if (status.ok() && response.success()) {
    std::cout << "Topic '" << topic << "' created successfully with " 
              << partitions << " partitions and replication factor " 
              << replication_factor << std::endl;
    return 0;
  } else {
    std::cerr << "Failed to create topic: ";
    if (status.ok()) {
      std::cerr << response.error_message() << std::endl;
    } else {
      std::cerr << status.error_message() << std::endl;
    }
    return 1;
  }
}

int RunDescribeTopic(int argc, char* argv[]) {
  // Parse command line arguments
  std::string controller_host = "localhost";
  int controller_port = 9093;
  std::string topic;
  
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintDescribeTopicHelp();
      return 0;
    } else if (arg == "--controller" && i + 1 < argc) {
      controller_host = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      controller_port = std::stoi(argv[++i]);
    } else if (arg == "--topic" && i + 1 < argc) {
      topic = argv[++i];
    }
  }
  
  if (topic.empty()) {
    std::cerr << "Error: --topic is required" << std::endl;
    PrintDescribeTopicHelp();
    return 1;
  }
  
  // Create gRPC channel
  std::string server_address = controller_host + ":" + std::to_string(controller_port);
  auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  auto stub = streamit::v1::Controller::NewStub(channel);
  
  // Describe topic request
  streamit::v1::DescribeTopicRequest request;
  request.set_topic(topic);
  
  streamit::v1::DescribeTopicResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub->DescribeTopic(&context, request, &response);
  
  if (status.ok()) {
    std::cout << "Topic: " << response.topic() << std::endl;
    std::cout << "Partitions:" << std::endl;
    
    for (const auto& partition : response.partitions()) {
      std::cout << "  Partition " << partition.partition() 
                << " (Leader: " << partition.leader() 
                << ", HW: " << partition.high_watermark() << ")";
      
      if (!partition.replicas().empty()) {
        std::cout << " [Replicas: ";
        for (int i = 0; i < partition.replicas().size(); ++i) {
          if (i > 0) std::cout << ", ";
          std::cout << partition.replicas(i);
        }
        std::cout << "]";
      }
      std::cout << std::endl;
    }
    return 0;
  } else {
    std::cerr << "Failed to describe topic: " << status.error_message() << std::endl;
    return 1;
  }
}

int RunListTopics(int argc, char* argv[]) {
  // Parse command line arguments
  std::string controller_host = "localhost";
  int controller_port = 9093;
  
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      PrintListTopicsHelp();
      return 0;
    } else if (arg == "--controller" && i + 1 < argc) {
      controller_host = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      controller_port = std::stoi(argv[++i]);
    }
  }
  
  // For simplicity, just list some default topics
  // In a real implementation, this would query the controller
  std::cout << "Topics:" << std::endl;
  std::cout << "  orders" << std::endl;
  std::cout << "  events" << std::endl;
  
  return 0;
}

void PrintAdminHelp() {
  std::cout << "Usage: streamit_cli admin <command> [options]\n"
            << "\n"
            << "Commands:\n"
            << "  create-topic     Create a new topic\n"
            << "  describe-topic   Describe a topic\n"
            << "  list-topics      List all topics\n"
            << "\n"
            << "Use 'streamit_cli admin <command> --help' for command-specific help.\n";
}

void PrintCreateTopicHelp() {
  std::cout << "Usage: streamit_cli admin create-topic [options]\n"
            << "\n"
            << "Options:\n"
            << "  --controller HOST     Controller hostname (default: localhost)\n"
            << "  --port PORT           Controller port (default: 9093)\n"
            << "  --topic TOPIC         Topic name (required)\n"
            << "  --partitions NUM      Number of partitions (default: 1)\n"
            << "  --replication-factor NUM  Replication factor (default: 1)\n"
            << "  --help, -h            Show this help message\n";
}

void PrintDescribeTopicHelp() {
  std::cout << "Usage: streamit_cli admin describe-topic [options]\n"
            << "\n"
            << "Options:\n"
            << "  --controller HOST     Controller hostname (default: localhost)\n"
            << "  --port PORT           Controller port (default: 9093)\n"
            << "  --topic TOPIC         Topic name (required)\n"
            << "  --help, -h            Show this help message\n";
}

void PrintListTopicsHelp() {
  std::cout << "Usage: streamit_cli admin list-topics [options]\n"
            << "\n"
            << "Options:\n"
            << "  --controller HOST     Controller hostname (default: localhost)\n"
            << "  --port PORT           Controller port (default: 9093)\n"
            << "  --help, -h            Show this help message\n";
}

}

