#pragma once

#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <string>

namespace streamit::common {

// Custom status codes for StreamIt
enum class StreamItErrorCode {
  kOk = 0,
  kInvalidArgument = 1,
  kNotFound = 2,
  kAlreadyExists = 3,
  kPermissionDenied = 4,
  kResourceExhausted = 5,
  kFailedPrecondition = 6,
  kOutOfRange = 7,
  kUnimplemented = 8,
  kInternal = 9,
  kUnavailable = 10,
  kDataLoss = 11,
  kUnauthenticated = 12,
  kThrottled = 13,
  kCorruptedData = 14,
  kNotLeader = 15,
  kReplicationTimeout = 16,
};

// Convert StreamIt error code to absl::StatusCode
absl::StatusCode ToAbslStatusCode(StreamItErrorCode code) noexcept;

// Create status with custom error code
absl::Status MakeStatus(StreamItErrorCode code, const std::string& message = "");

// Check if status represents a retryable error
bool IsRetryable(const absl::Status& status) noexcept;

// Check if status represents a client error (not retryable)
bool IsClientError(const absl::Status& status) noexcept;

} // namespace streamit::common
