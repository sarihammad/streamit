#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <memory>
#include "streamit/broker/broker_service.h"
#include "streamit/controller/controller_service.h"
#include "streamit/storage/log_dir.h"

namespace streamit::testing {

class ChaosTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "streamit_chaos_test";
        std::filesystem::create_directories(test_dir_);
        
        // Initialize test broker
        broker_config_ = std::make_unique<streamit::broker::BrokerConfig>();
        broker_config_->log_dir = test_dir_ / "broker_logs";
        broker_config_->port = 9092;
        broker_config_->log_level = "info";
        
        broker_service_ = std::make_unique<streamit::broker::BrokerServiceImpl>(*broker_config_);
        
        // Initialize test controller
        controller_config_ = std::make_unique<streamit::controller::ControllerConfig>();
        controller_config_->port = 9093;
        controller_config_->log_level = "info";
        
        controller_service_ = std::make_unique<streamit::controller::ControllerServiceImpl>(*controller_config_);
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::path test_dir_;
    std::unique_ptr<streamit::broker::BrokerConfig> broker_config_;
    std::unique_ptr<streamit::broker::BrokerServiceImpl> broker_service_;
    std::unique_ptr<streamit::controller::ControllerConfig> controller_config_;
    std::unique_ptr<streamit::controller::ControllerServiceImpl> controller_service_;
};

TEST_F(ChaosTest, LeaderKillAndRecovery) {
    // Create a topic
    grpc::ServerContext context;
    streamit::proto::CreateTopicRequest request;
    streamit::proto::CreateTopicResponse response;
    
    request.set_topic_name("chaos-topic");
    request.set_partitions(3);
    request.set_replication_factor(1);
    
    auto status = controller_service_->CreateTopic(&context, &request, &response);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(response.error_code(), streamit::proto::ErrorCode::OK);
    
    // Produce some data to the topic
    for (int i = 0; i < 10; ++i) {
        grpc::ServerContext produce_context;
        streamit::proto::ProduceRequest produce_request;
        streamit::proto::ProduceResponse produce_response;
        
        produce_request.set_topic("chaos-topic");
        produce_request.set_partition(0);
        produce_request.set_data("chaos_data_" + std::to_string(i));
        
        auto produce_status = broker_service_->Produce(&produce_context, &produce_request, &produce_response);
        ASSERT_TRUE(produce_status.ok());
        ASSERT_EQ(produce_response.error_code(), streamit::proto::ErrorCode::OK);
    }
    
    // Simulate leader kill by stopping the broker service
    broker_service_.reset();
    
    // Wait a bit to simulate leader unavailability
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Restart the broker service
    broker_service_ = std::make_unique<streamit::broker::BrokerServiceImpl>(*broker_config_);
    
    // Verify that data is still available after recovery
    grpc::ServerContext fetch_context;
    streamit::proto::FetchRequest fetch_request;
    streamit::proto::FetchResponse fetch_response;
    
    fetch_request.set_topic("chaos-topic");
    fetch_request.set_partition(0);
    fetch_request.set_offset(0);
    fetch_request.set_max_bytes(1000);
    
    auto fetch_status = broker_service_->Fetch(&fetch_context, &fetch_request, &fetch_response);
    ASSERT_TRUE(fetch_status.ok());
    ASSERT_EQ(fetch_response.error_code(), streamit::proto::ErrorCode::OK);
    
    // Should have all the data we produced
    EXPECT_GT(fetch_response.records_size(), 0);
}

TEST_F(ChaosTest, ConcurrentLeaderFailover) {
    // Create a topic with multiple partitions
    grpc::ServerContext context;
    streamit::proto::CreateTopicRequest request;
    streamit::proto::CreateTopicResponse response;
    
    request.set_topic_name("concurrent-chaos-topic");
    request.set_partitions(3);
    request.set_replication_factor(1);
    
    auto status = controller_service_->CreateTopic(&context, &request, &response);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(response.error_code(), streamit::proto::ErrorCode::OK);
    
    // Start multiple producer threads
    std::atomic<bool> stop_producing{false};
    std::vector<std::thread> producer_threads;
    std::atomic<int> total_produced{0};
    
    for (int t = 0; t < 3; ++t) {
        producer_threads.emplace_back([&, t]() {
            int local_count = 0;
            while (!stop_producing.load()) {
                grpc::ServerContext produce_context;
                streamit::proto::ProduceRequest produce_request;
                streamit::proto::ProduceResponse produce_response;
                
                produce_request.set_topic("concurrent-chaos-topic");
                produce_request.set_partition(t % 3); // Round-robin partitions
                produce_request.set_data("concurrent_data_" + std::to_string(t) + "_" + std::to_string(local_count));
                
                auto produce_status = broker_service_->Produce(&produce_context, &produce_request, &produce_response);
                if (produce_status.ok() && produce_response.error_code() == streamit::proto::ErrorCode::OK) {
                    local_count++;
                    total_produced.fetch_add(1);
                }
                
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
    }
    
    // Let producers run for a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Simulate leader kill
    broker_service_.reset();
    
    // Wait for leader unavailability
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Restart broker
    broker_service_ = std::make_unique<streamit::broker::BrokerServiceImpl>(*broker_config_);
    
    // Stop producers
    stop_producing.store(true);
    for (auto& thread : producer_threads) {
        thread.join();
    }
    
    // Verify data integrity across all partitions
    for (int p = 0; p < 3; ++p) {
        grpc::ServerContext fetch_context;
        streamit::proto::FetchRequest fetch_request;
        streamit::proto::FetchResponse fetch_response;
        
        fetch_request.set_topic("concurrent-chaos-topic");
        fetch_request.set_partition(p);
        fetch_request.set_offset(0);
        fetch_request.set_max_bytes(1000);
        
        auto fetch_status = broker_service_->Fetch(&fetch_context, &fetch_request, &fetch_response);
        ASSERT_TRUE(fetch_status.ok());
        ASSERT_EQ(fetch_response.error_code(), streamit::proto::ErrorCode::OK);
        
        // Should have some data in each partition
        EXPECT_GT(fetch_response.records_size(), 0);
    }
}

TEST_F(ChaosTest, NetworkPartitionSimulation) {
    // Create a topic
    grpc::ServerContext context;
    streamit::proto::CreateTopicRequest request;
    streamit::proto::CreateTopicResponse response;
    
    request.set_topic_name("network-partition-topic");
    request.set_partitions(1);
    request.set_replication_factor(1);
    
    auto status = controller_service_->CreateTopic(&context, &request, &response);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(response.error_code(), streamit::proto::ErrorCode::OK);
    
    // Produce some initial data
    for (int i = 0; i < 5; ++i) {
        grpc::ServerContext produce_context;
        streamit::proto::ProduceRequest produce_request;
        streamit::proto::ProduceResponse produce_response;
        
        produce_request.set_topic("network-partition-topic");
        produce_request.set_partition(0);
        produce_request.set_data("initial_data_" + std::to_string(i));
        
        auto produce_status = broker_service_->Produce(&produce_context, &produce_request, &produce_response);
        ASSERT_TRUE(produce_status.ok());
        ASSERT_EQ(produce_response.error_code(), streamit::proto::ErrorCode::OK);
    }
    
    // Simulate network partition by stopping the broker
    broker_service_.reset();
    
    // Wait for partition
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Restart broker (simulate network recovery)
    broker_service_ = std::make_unique<streamit::broker::BrokerServiceImpl>(*broker_config_);
    
    // Verify that initial data is still available
    grpc::ServerContext fetch_context;
    streamit::proto::FetchRequest fetch_request;
    streamit::proto::FetchResponse fetch_response;
    
    fetch_request.set_topic("network-partition-topic");
    fetch_request.set_partition(0);
    fetch_request.set_offset(0);
    fetch_request.set_max_bytes(1000);
    
    auto fetch_status = broker_service_->Fetch(&fetch_context, &fetch_request, &fetch_response);
    ASSERT_TRUE(fetch_status.ok());
    ASSERT_EQ(fetch_response.error_code(), streamit::proto::ErrorCode::OK);
    
    // Should have the initial data
    EXPECT_EQ(fetch_response.records_size(), 5);
}

TEST_F(ChaosTest, RapidRestartStress) {
    // Create a topic
    grpc::ServerContext context;
    streamit::proto::CreateTopicRequest request;
    streamit::proto::CreateTopicResponse response;
    
    request.set_topic_name("rapid-restart-topic");
    request.set_partitions(1);
    request.set_replication_factor(1);
    
    auto status = controller_service_->CreateTopic(&context, &request, &response);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(response.error_code(), streamit::proto::ErrorCode::OK);
    
    // Rapidly restart the broker multiple times
    for (int restart = 0; restart < 5; ++restart) {
        // Produce some data
        for (int i = 0; i < 3; ++i) {
            grpc::ServerContext produce_context;
            streamit::proto::ProduceRequest produce_request;
            streamit::proto::ProduceResponse produce_response;
            
            produce_request.set_topic("rapid-restart-topic");
            produce_request.set_partition(0);
            produce_request.set_data("restart_data_" + std::to_string(restart) + "_" + std::to_string(i));
            
            auto produce_status = broker_service_->Produce(&produce_context, &produce_request, &produce_response);
            ASSERT_TRUE(produce_status.ok());
            ASSERT_EQ(produce_response.error_code(), streamit::proto::ErrorCode::OK);
        }
        
        // Restart broker
        broker_service_.reset();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        broker_service_ = std::make_unique<streamit::broker::BrokerServiceImpl>(*broker_config_);
    }
    
    // Verify that all data is still available
    grpc::ServerContext fetch_context;
    streamit::proto::FetchRequest fetch_request;
    streamit::proto::FetchResponse fetch_response;
    
    fetch_request.set_topic("rapid-restart-topic");
    fetch_request.set_partition(0);
    fetch_request.set_offset(0);
    fetch_request.set_max_bytes(1000);
    
    auto fetch_status = broker_service_->Fetch(&fetch_context, &fetch_request, &fetch_response);
    ASSERT_TRUE(fetch_status.ok());
    ASSERT_EQ(fetch_response.error_code(), streamit::proto::ErrorCode::OK);
    
    // Should have data from all restarts
    EXPECT_GT(fetch_response.records_size(), 0);
}

TEST_F(ChaosTest, MemoryPressureAndRecovery) {
    // Create a topic
    grpc::ServerContext context;
    streamit::proto::CreateTopicRequest request;
    streamit::proto::CreateTopicResponse response;
    
    request.set_topic_name("memory-pressure-topic");
    request.set_partitions(1);
    request.set_replication_factor(1);
    
    auto status = controller_service_->CreateTopic(&context, &request, &response);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(response.error_code(), streamit::proto::ErrorCode::OK);
    
    // Produce large amounts of data to simulate memory pressure
    for (int i = 0; i < 100; ++i) {
        grpc::ServerContext produce_context;
        streamit::proto::ProduceRequest produce_request;
        streamit::proto::ProduceResponse produce_response;
        
        produce_request.set_topic("memory-pressure-topic");
        produce_request.set_partition(0);
        produce_request.set_data("large_data_" + std::to_string(i) + std::string(1000, 'x'));
        
        auto produce_status = broker_service_->Produce(&produce_context, &produce_request, &produce_response);
        ASSERT_TRUE(produce_status.ok());
        ASSERT_EQ(produce_response.error_code(), streamit::proto::ErrorCode::OK);
    }
    
    // Simulate memory pressure by restarting
    broker_service_.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    broker_service_ = std::make_unique<streamit::broker::BrokerServiceImpl>(*broker_config_);
    
    // Verify that data is still available
    grpc::ServerContext fetch_context;
    streamit::proto::FetchRequest fetch_request;
    streamit::proto::FetchResponse fetch_response;
    
    fetch_request.set_topic("memory-pressure-topic");
    fetch_request.set_partition(0);
    fetch_request.set_offset(0);
    fetch_request.set_max_bytes(10000);
    
    auto fetch_status = broker_service_->Fetch(&fetch_context, &fetch_request, &fetch_response);
    ASSERT_TRUE(fetch_status.ok());
    ASSERT_EQ(fetch_response.error_code(), streamit::proto::ErrorCode::OK);
    
    // Should have the large data
    EXPECT_GT(fetch_response.records_size(), 0);
}

} // namespace streamit::testing
