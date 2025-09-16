#include "producer.h"
#include "consumer.h"
#include "admin.h"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <iostream>
#include <string>

namespace {
void SetupLogging() {
  auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  auto logger = std::make_shared<spdlog::logger>("streamit-cli", console_sink);
  logger->set_level(spdlog::level::info);
  spdlog::set_default_logger(logger);
}

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " <command> [options]\n"
            << "\n"
            << "Commands:\n"
            << "  produce    Produce messages to a topic\n"
            << "  consume    Consume messages from a topic\n"
            << "  admin      Administrative operations\n"
            << "\n"
            << "Use '" << program_name << " <command> --help' for command-specific help.\n";
}

} 

int main(int argc, char* argv[]) {
  SetupLogging();
  
  if (argc < 2) {
    PrintUsage(argv[0]);
    return 1;
  }
  
  std::string command = argv[1];
  
  if (command == "produce") {
    return streamit::cli::RunProducer(argc - 1, argv + 1);
  } else if (command == "consume") {
    return streamit::cli::RunConsumer(argc - 1, argv + 1);
  } else if (command == "admin") {
    return streamit::cli::RunAdmin(argc - 1, argv + 1);
  } else {
    std::cerr << "Unknown command: " << command << std::endl;
    PrintUsage(argv[0]);
    return 1;
  }
}

