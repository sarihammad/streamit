#include <gtest/gtest.h>
#include "streamit/common/status.h"
#include "streamit/common/result.h"
#include "streamit/common/crc32.h"

namespace streamit::common {
namespace {

TEST(StatusTest, MakeStatus) {
  auto status = MakeStatus(StreamItErrorCode::kOk);
  EXPECT_TRUE(status.ok());
  
  auto error_status = MakeStatus(StreamItErrorCode::kInvalidArgument, "test error");
  EXPECT_FALSE(error_status.ok());
  EXPECT_EQ(error_status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(error_status.message(), "test error");
}

TEST(StatusTest, IsRetryable) {
  auto retryable_status = MakeStatus(StreamItErrorCode::kThrottled, "throttled");
  EXPECT_TRUE(IsRetryable(retryable_status));
  
  auto client_error_status = MakeStatus(StreamItErrorCode::kInvalidArgument, "invalid");
  EXPECT_FALSE(IsRetryable(client_error_status));
}

TEST(StatusTest, IsClientError) {
  auto client_error_status = MakeStatus(StreamItErrorCode::kInvalidArgument, "invalid");
  EXPECT_TRUE(IsClientError(client_error_status));
  
  auto retryable_status = MakeStatus(StreamItErrorCode::kThrottled, "throttled");
  EXPECT_FALSE(IsClientError(retryable_status));
}

TEST(ResultTest, Ok) {
  auto result = Ok(42);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value(), 42);
}

TEST(ResultTest, Error) {
  auto result = Error<int>(absl::StatusCode::kInvalidArgument, "test error");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST(ResultTest, UnwrapOr) {
  auto ok_result = Ok(42);
  EXPECT_EQ(UnwrapOr(ok_result, 0), 42);
  
  auto error_result = Error<int>(absl::StatusCode::kInvalidArgument, "error");
  EXPECT_EQ(UnwrapOr(error_result, 0), 0);
}

TEST(Crc32Test, Compute) {
  std::string data = "hello world";
  uint32_t crc1 = Crc32::Compute(data);
  uint32_t crc2 = Crc32::Compute(data);
  EXPECT_EQ(crc1, crc2);
  
  std::string different_data = "hello world!";
  uint32_t crc3 = Crc32::Compute(different_data);
  EXPECT_NE(crc1, crc3);
}

TEST(Crc32Test, Verify) {
  std::string data = "hello world";
  uint32_t crc = Crc32::Compute(data);
  EXPECT_TRUE(Crc32::Verify(data, crc));
  
  std::string corrupted_data = "hello worlx";
  EXPECT_FALSE(Crc32::Verify(corrupted_data, crc));
}

TEST(Crc32Test, EmptyData) {
  std::string empty_data = "";
  uint32_t crc = Crc32::Compute(empty_data);
  EXPECT_TRUE(Crc32::Verify(empty_data, crc));
}

} 
}

