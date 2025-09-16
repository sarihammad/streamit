#pragma once

#include <absl/status/statusor.h>
#include <concepts>
#include <type_traits>

namespace streamit::common {

// Result type alias for better readability
template <typename T>
using Result = absl::StatusOr<T>;

// Helper to create successful results
template <typename T>
Result<T> Ok(T&& value) {
  return Result<T>(std::forward<T>(value));
}

// Helper to create error results
template <typename T>
Result<T> Error(absl::Status status) {
  return Result<T>(std::move(status));
}

// Helper to create error results from error code
template <typename T>
Result<T> Error(absl::StatusCode code, const std::string& message) {
  return Result<T>(absl::Status(code, message));
}

// Concept for types that can be used in Result
template <typename T>
concept ResultValue = !std::is_same_v<T, absl::Status>;

// Helper to unwrap Result with default value
template <typename T>
T UnwrapOr(const Result<T>& result, T&& default_value) {
  return result.ok() ? result.value() : std::forward<T>(default_value);
}

// Helper to unwrap Result with default factory
template <typename T, typename F>
T UnwrapOrElse(const Result<T>& result, F&& factory) {
  return result.ok() ? result.value() : factory();
}

}

