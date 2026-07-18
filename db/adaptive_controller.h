#ifndef STORAGE_LEVELDB_DB_ADAPTIVE_CONTROLLER_H_
#define STORAGE_LEVELDB_DB_ADAPTIVE_CONTROLLER_H_

#include <vector>
#include <unordered_map>
#include <set>
#include <cstdint>
#include "db/version_edit.h"

namespace leveldb {

struct MetricSnapshot {
  uint64_t timestamp; // microseconds
  int64_t readCount;
  int64_t writeCount;
};

class AdaptiveController {
 public:
  AdaptiveController() : last_global_switch_time_us_(0) {}
  ~AdaptiveController() = default;

  // Evaluates trend thresholds and determines if the SSTable strategy should switch.
  bool ShouldRewrite(FileMetaData* f, Strategy* out_strategy);

  // Cleans up the history map of obsolete file numbers to avoid memory leaks.
  void Cleanup(const std::set<uint64_t>& active_files);

  // AMETHYST: lifetime counts of each strategy-switch direction this
  // controller has triggered via ShouldRewrite, for benchmark
  // instrumentation (distinguishes "the adaptive mechanism actually fired"
  // from "the always-on tiered-read-path overhead" when comparing WA/RA
  // against stock). Only meaningful read under the same mutex ShouldRewrite
  // is called under (PickCompaction holds the DB mutex; see version_set.cc).
  void GetTransitionCounts(int64_t* tiered_to_leveled,
                          int64_t* leveled_to_tiered) const;

 private:
  void ComputeEMA(const std::vector<MetricSnapshot>& window, double* read_trend, double* write_trend);

  uint64_t last_global_switch_time_us_;
  std::unordered_map<uint64_t, std::vector<MetricSnapshot>> history_;
  int64_t tiered_to_leveled_transitions_ = 0;
  int64_t leveled_to_tiered_transitions_ = 0;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_ADAPTIVE_CONTROLLER_H_
