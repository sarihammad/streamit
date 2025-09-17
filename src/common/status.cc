#include "streamit/common/status.h"
#include <absl/status/status.h>

namespace streamit::common {

absl::StatusCode ToAbslStatusCode(StreamItErrorCode code) noexcept {
  switch (code) {
  case StreamItErrorCode::kOk:
    return absl::StatusCode::kOk;
  case StreamItErrorCode::kInvalidArgument:
    return absl::StatusCode::kInvalidArgument;
  case StreamItErrorCode::kNotFound:
    return absl::StatusCode::kNotFound;
  case StreamItErrorCode::kAlreadyExists:
    return absl::StatusCode::kAlreadyExists;
  case StreamItErrorCode::kPermissionDenied:
    return absl::StatusCode::kPermissionDenied;
  case StreamItErrorCode::kResourceExhausted:
    return absl::StatusCode::kResourceExhausted;
  case StreamItErrorCode::kFailedPrecondition:
    return absl::StatusCode::kFailedPrecondition;
  case StreamItErrorCode::kOutOfRange:
    return absl::StatusCode::kOutOfRange;
  case StreamItErrorCode::kUnimplemented:
    return absl::StatusCode::kUnimplemented;
  case StreamItErrorCode::kInternal:
    return absl::StatusCode::kInternal;
  case StreamItErrorCode::kUnavailable:
    return absl::StatusCode::kUnavailable;
  case StreamItErrorCode::kDataLoss:
    return absl::StatusCode::kDataLoss;
  case StreamItErrorCode::kUnauthenticated:
    return absl::StatusCode::kUnauthenticated;
  case StreamItErrorCode::kThrottled:
    return absl::StatusCode::kUnavailable; // Map to unavailable for retry
  case StreamItErrorCode::kCorruptedData:
    return absl::StatusCode::kDataLoss;
  case StreamItErrorCode::kNotLeader:
    return absl::StatusCode::kFailedPrecondition;
  case StreamItErrorCode::kReplicationTimeout:
    return absl::StatusCode::kDeadlineExceeded;
  default:
    return absl::StatusCode::kUnknown;
  }
}

absl::Status MakeStatus(StreamItErrorCode code, const std::string& message) {
  if (code == StreamItErrorCode::kOk) {
    return absl::OkStatus();
  }
  return absl::Status(ToAbslStatusCode(code), message);
}

bool IsRetryable(const absl::Status& status) noexcept {
  return status.code() == absl::StatusCode::kUnavailable || status.code() == absl::StatusCode::kDeadlineExceeded ||
         status.code() == absl::StatusCode::kResourceExhausted ||
         status.message().find("THROTTLED") != std::string::npos;
}

bool IsClientError(const absl::Status& status) noexcept {
  return status.code() == absl::StatusCode::kInvalidArgument || status.code() == absl::StatusCode::kNotFound ||
         status.code() == absl::StatusCode::kAlreadyExists || status.code() == absl::StatusCode::kPermissionDenied ||
         status.code() == absl::StatusCode::kFailedPrecondition || status.code() == absl::StatusCode::kOutOfRange ||
         status.code() == absl::StatusCode::kUnauthenticated;
}

} // namespace streamit::common
