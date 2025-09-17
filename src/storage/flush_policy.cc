#include "streamit/storage/flush_policy.h"
#include <algorithm>
#include <string>

namespace streamit::storage {

FlushPolicy ParseFlushPolicy(const std::string& policy_str) noexcept {
  std::string lower_policy = policy_str;
  std::transform(lower_policy.begin(), lower_policy.end(), lower_policy.begin(), ::tolower);

  if (lower_policy == "never") {
    return FlushPolicy::Never;
  } else if (lower_policy == "onroll") {
    return FlushPolicy::OnRoll;
  } else if (lower_policy == "eachbatch") {
    return FlushPolicy::EachBatch;
  } else {
    return FlushPolicy::OnRoll; // Default
  }
}

std::string ToString(FlushPolicy policy) noexcept {
  switch (policy) {
  case FlushPolicy::Never:
    return "never";
  case FlushPolicy::OnRoll:
    return "onroll";
  case FlushPolicy::EachBatch:
    return "eachbatch";
  default:
    return "onroll";
  }
}

} // namespace streamit::storage
