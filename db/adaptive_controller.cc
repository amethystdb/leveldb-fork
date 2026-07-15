#include "db/adaptive_controller.h"
#include "leveldb/env.h"

namespace leveldb {

void AdaptiveController::ComputeEMA(const std::vector<MetricSnapshot>& window, double* read_trend, double* write_trend) {
  *read_trend = 0.0;
  *write_trend = 0.0;
  if (window.size() < 2) return;

  double r_ema = 0.0;
  double w_ema = 0.0;
  double alpha = 0.2;

  for (size_t i = 1; i < window.size(); i++) {
    double r_delta = static_cast<double>(window[i].readCount - window[i-1].readCount);
    double w_delta = static_cast<double>(window[i].writeCount - window[i-1].writeCount);
    if (r_delta < 0) r_delta = 0;
    if (w_delta < 0) w_delta = 0;

    if (i == 1) {
      r_ema = r_delta;
      w_ema = w_delta;
    } else {
      r_ema = alpha * r_delta + (1.0 - alpha) * r_ema;
      w_ema = alpha * w_delta + (1.0 - alpha) * w_ema;
    }
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
