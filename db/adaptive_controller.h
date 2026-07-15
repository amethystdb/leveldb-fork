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

 private:
  void ComputeEMA(const std::vector<MetricSnapshot>& window, double* read_trend, double* write_trend);

  uint64_t last_global_switch_time_us_;
  std::unordered_map<uint64_t, std::vector<MetricSnapshot>> history_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_ADAPTIVE_CONTROLLER_H_
