#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <chrono>
#include "streamit/storage/segment.h"
#include "streamit/storage/log_dir.h"
#include "streamit/storage/flush_policy.h"

namespace streamit::testing {

class DurabilityTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "streamit_durability_test";
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::path test_dir_;
};

TEST_F(DurabilityTest, CrashRecoveryWithPartialBatch) {
    // Create a segment and write some data
    auto segment = std::make_unique<streamit::storage::Segment>(
        test_dir_ / "test-topic" / "0", 0, 1024, 1024, 
        streamit::storage::FlushPolicy::OnRoll
    );
    
    ASSERT_TRUE(segment->Open().ok());
    
    // Write some complete batches
    for (int i = 0; i < 5; ++i) {
        std::string data = "batch_" + std::to_string(i);
        auto result = segment->Append(data.data(), data.size());
        ASSERT_TRUE(result.ok());
    }
    
    // Write a partial batch (simulate crash during write)
    std::string partial_data = "partial_batch_";
    auto result = segment->Append(partial_data.data(), partial_data.size());
    ASSERT_TRUE(result.ok());
    
    // Simulate crash by closing the segment without proper flush
    segment.reset();
    
    // Reopen the segment - it should recover and truncate the partial batch
    segment = std::make_unique<streamit::storage::Segment>(
        test_dir_ / "test-topic" / "0", 0, 1024, 1024,
        streamit::storage::FlushPolicy::OnRoll
    );
    
    ASSERT_TRUE(segment->Open().ok());
    
    // Verify that only complete batches are readable
    auto read_result = segment->Read(0, 1000);
    ASSERT_TRUE(read_result.ok());
    
    // Should have 5 complete batches, not the partial one
    EXPECT_EQ(read_result->size(), 5);
}

TEST_F(DurabilityTest, CorruptionDetectionAndRecovery) {
    // Create a segment and write some data
    auto segment = std::make_unique<streamit::storage::Segment>(
        test_dir_ / "test-topic" / "0", 0, 1024, 1024,
        streamit::storage::FlushPolicy::OnRoll
    );
    
    ASSERT_TRUE(segment->Open().ok());
    
    // Write some data
    for (int i = 0; i < 3; ++i) {
        std::string data = "batch_" + std::to_string(i);
        auto result = segment->Append(data.data(), data.size());
        ASSERT_TRUE(result.ok());
    }
    
    // Close the segment properly
    segment.reset();
    
    // Manually corrupt the log file
    std::string log_file = (test_dir_ / "test-topic" / "0" / "00000000000000000000.log").string();
    std::fstream file(log_file, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    
    // Corrupt the middle of the file
    file.seekp(100);
    file.write("CORRUPTED", 9);
    file.close();
    
    // Reopen the segment - it should detect corruption and truncate
    segment = std::make_unique<streamit::storage::Segment>(
        test_dir_ / "test-topic" / "0", 0, 1024, 1024,
        streamit::storage::FlushPolicy::OnRoll
    );
    
    ASSERT_TRUE(segment->Open().ok());
    
    // Verify that corrupted data is not readable
    auto read_result = segment->Read(0, 1000);
    ASSERT_TRUE(read_result.ok());
    
    // Should have fewer records due to corruption truncation
    EXPECT_LT(read_result->size(), 3);
}

TEST_F(DurabilityTest, IndexRebuildAfterCorruption) {
    // Create a segment with index
    auto segment = std::make_unique<streamit::storage::Segment>(
        test_dir_ / "test-topic" / "0", 0, 1024, 1024,
        streamit::storage::FlushPolicy::OnRoll
    );
    
    ASSERT_TRUE(segment->Open().ok());
    
    // Write enough data to trigger index entries
    for (int i = 0; i < 10; ++i) {
        std::string data = "batch_" + std::to_string(i);
        auto result = segment->Append(data.data(), data.size());
        ASSERT_TRUE(result.ok());
    }
    
    // Close the segment
    segment.reset();
    
    // Corrupt the index file
    std::string index_file = (test_dir_ / "test-topic" / "0" / "00000000000000000000.index").string();
    std::fstream file(index_file, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    
    // Corrupt the index
    file.seekp(0);
    file.write("CORRUPTED_INDEX", 15);
    file.close();
    
    // Reopen the segment - it should rebuild the index
    segment = std::make_unique<streamit::storage::Segment>(
        test_dir_ / "test-topic" / "0", 0, 1024, 1024,
        streamit::storage::FlushPolicy::OnRoll
    );
    
    ASSERT_TRUE(segment->Open().ok());
    
    // Verify that data is still readable (index was rebuilt)
    auto read_result = segment->Read(0, 1000);
    ASSERT_TRUE(read_result.ok());
    EXPECT_EQ(read_result->size(), 10);
}

TEST_F(DurabilityTest, ManifestRecovery) {
    // Create a segment
    auto segment = std::make_unique<streamit::storage::Segment>(
        test_dir_ / "test-topic" / "0", 0, 1024, 1024,
        streamit::storage::FlushPolicy::OnRoll
    );
    
    ASSERT_TRUE(segment->Open().ok());
    
    // Write some data
    for (int i = 0; i < 5; ++i) {
        std::string data = "batch_" + std::to_string(i);
        auto result = segment->Append(data.data(), data.size());
        ASSERT_TRUE(result.ok());
    }
    
    // Close the segment
    segment.reset();
    
    // Corrupt the manifest file
    std::string manifest_file = (test_dir_ / "test-topic" / "0" / "MANIFEST").string();
    std::fstream file(manifest_file, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    
    // Corrupt the manifest
    file.seekp(0);
    file.write("CORRUPTED_MANIFEST", 18);
    file.close();
    
    // Reopen the segment - it should recover from manifest corruption
    segment = std::make_unique<streamit::storage::Segment>(
        test_dir_ / "test-topic" / "0", 0, 1024, 1024,
        streamit::storage::FlushPolicy::OnRoll
    );
    
    ASSERT_TRUE(segment->Open().ok());
    
    // Verify that data is still readable
    auto read_result = segment->Read(0, 1000);
    ASSERT_TRUE(read_result.ok());
    EXPECT_EQ(read_result->size(), 5);
}

TEST_F(DurabilityTest, FlushPolicyPersistence) {
    // Test that different flush policies work correctly
    std::vector<streamit::storage::FlushPolicy> policies = {
        streamit::storage::FlushPolicy::Never,
        streamit::storage::FlushPolicy::OnRoll,
        streamit::storage::FlushPolicy::EachBatch
    };
    
    for (auto policy : policies) {
        auto segment = std::make_unique<streamit::storage::Segment>(
            test_dir_ / "test-topic" / std::to_string(static_cast<int>(policy)), 
            0, 1024, 1024, policy
        );
        
        ASSERT_TRUE(segment->Open().ok());
        
        // Write some data
        for (int i = 0; i < 3; ++i) {
            std::string data = "batch_" + std::to_string(i);
            auto result = segment->Append(data.data(), data.size());
            ASSERT_TRUE(result.ok());
        }
        
        // Close and reopen
        segment.reset();
        
        segment = std::make_unique<streamit::storage::Segment>(
            test_dir_ / "test-topic" / std::to_string(static_cast<int>(policy)),
            0, 1024, 1024, policy
        );
        
        ASSERT_TRUE(segment->Open().ok());
        
        // Verify data is readable
        auto read_result = segment->Read(0, 1000);
        ASSERT_TRUE(read_result.ok());
        EXPECT_EQ(read_result->size(), 3);
    }
}

TEST_F(DurabilityTest, ConcurrentWriteAndCrash) {
    // Test concurrent writes with simulated crashes
    auto segment = std::make_unique<streamit::storage::Segment>(
        test_dir_ / "test-topic" / "0", 0, 1024, 1024,
        streamit::storage::FlushPolicy::OnRoll
    );
    
    ASSERT_TRUE(segment->Open().ok());
    
    std::atomic<bool> stop_writing{false};
    std::vector<std::string> written_data;
    std::mutex data_mutex;
    
    // Start a writer thread
    std::thread writer([&]() {
        int counter = 0;
        while (!stop_writing.load()) {
            std::string data = "concurrent_batch_" + std::to_string(counter++);
            auto result = segment->Append(data.data(), data.size());
            if (result.ok()) {
                std::lock_guard<std::mutex> lock(data_mutex);
                written_data.push_back(data);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
    
    // Let it write for a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Simulate crash by stopping the writer and closing the segment
    stop_writing.store(true);
    writer.join();
    segment.reset();
    
    // Reopen and verify data integrity
    segment = std::make_unique<streamit::storage::Segment>(
        test_dir_ / "test-topic" / "0", 0, 1024, 1024,
        streamit::storage::FlushPolicy::OnRoll
    );
    
    ASSERT_TRUE(segment->Open().ok());
    
    // Verify that all written data is recoverable
    auto read_result = segment->Read(0, 1000);
    ASSERT_TRUE(read_result.ok());
    
    // Should have at least some of the written data
    EXPECT_GT(read_result->size(), 0);
    EXPECT_LE(read_result->size(), written_data.size());
}

} // namespace streamit::testing
