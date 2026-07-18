#include "db/adaptive_controller.h"
#include "leveldb/env.h"

namespace leveldb {

namespace {
// AMETHYST: minimum number of distinct polling samples a file's history
// must hold before ShouldRewrite will trust the EMA enough to switch
// strategy. Guards against the EMA cold-start transient -- see the
// warm-up-guard comment in ShouldRewrite for why this recurs continuously
// here rather than only once at DB open.
constexpr size_t kMinWindowsForSwitch = 4;
}  // namespace

void AdaptiveController::ComputeEMA(const std::vector<MetricSnapshot>& window, double* read_trend, double* write_trend) {
  *read_trend = 0.0;
  *write_trend = 0.0;
  if (window.size() < 2) return;

  // Seed both EMAs at zero and let them warm up from the first delta
  // onward, rather than initializing r_ema/w_ema directly from the first
  // sample's raw delta. Seeding from the first sample makes that single
  // (often noisy) delta count for far more than alpha's intended weight,
  // biasing the estimate right when there's the least history to trust.
  // Paired with the kMinWindowsForSwitch guard in ShouldRewrite so the
  // still-warming-up early values are never actually acted on.
  double r_ema = 0.0;
  double w_ema = 0.0;
  double alpha = 0.2;

  for (size_t i = 1; i < window.size(); i++) {
    double r_delta = static_cast<double>(window[i].readCount - window[i-1].readCount);
    double w_delta = static_cast<double>(window[i].writeCount - window[i-1].writeCount);
    if (r_delta < 0) r_delta = 0;
    if (w_delta < 0) w_delta = 0;

    r_ema = alpha * r_delta + (1.0 - alpha) * r_ema;
    w_ema = alpha * w_delta + (1.0 - alpha) * w_ema;
  }

  double total_ema = r_ema + w_ema;
  if (total_ema > 0.0) {
    *read_trend = r_ema / total_ema;
    *write_trend = w_ema / total_ema;
  }
}

bool AdaptiveController::ShouldRewrite(FileMetaData* f, Strategy* out_strategy) {
  uint64_t now = Env::Default()->NowMicros();
  int64_t current_read = f->readCount.load(std::memory_order_relaxed);
  int64_t current_write = f->writeCount.load(std::memory_order_relaxed);

  auto& window = history_[f->number];
  if (window.empty() || window.back().readCount != current_read || window.back().writeCount != current_write) {
    if (window.size() >= 5) {
      window.erase(window.begin());
    }
    window.push_back({now, current_read, current_write});
  }

  if (window.size() < 2) {
    return false;
  }

  // AMETHYST: warm-up guard against EMA cold-start bias. History is keyed
  // by file number (history_), and file numbers change on every
  // compaction -- so the EMA re-seeds from empty on every rewrite, not
  // just once at DB open. In a compaction-heavy workload that means the
  // cold-start transient recurs continuously: combined with the poll
  // interval (Options::adaptive_poll_interval_ms), a single noisy early
  // window can bias the controller for several poll cycles after every
  // file rewrite. Require a minimum number of accumulated samples before
  // trusting the EMA enough to act on it; below that, never switch.
  // This guard (and the zero-seeded EMA above) become much less critical
  // once stats are region-keyed instead of file-keyed -- a long-lived
  // per-region estimator wouldn't re-seed on compaction at all -- but
  // remain correct, cheap defensive guards for genuinely new regions
  // even after that lands.
  if (window.size() < kMinWindowsForSwitch) {
    return false;
  }

  // 2-second global minimum switch interval (2,000,000 microseconds)
  if (now - last_global_switch_time_us_ < 2000000) {
    return false;
  }

  double read_trend = 0.0;
  double write_trend = 0.0;
  ComputeEMA(window, &read_trend, &write_trend);

  if (f->strategy == kTiered) {
    if (read_trend > 0.3 && current_read > 500) {
      *out_strategy = kLeveled;
      last_global_switch_time_us_ = now;
      return true;
    }
  } else if (f->strategy == kLeveled) {
    if (write_trend > 0.3 && current_write > 10) {
      *out_strategy = kTiered;
      last_global_switch_time_us_ = now;
      return true;
    }
  }

  return false;
}

void AdaptiveController::Cleanup(const std::set<uint64_t>& active_files) {
  for (auto it = history_.begin(); it != history_.end(); ) {
    if (active_files.find(it->first) == active_files.end()) {
      it = history_.erase(it);
    } else {
      ++it;
    }
  }
}

} // namespace leveldb
