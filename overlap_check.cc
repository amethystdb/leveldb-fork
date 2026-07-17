// Diagnostic: determine whether tiered SSTables at L1+ actually accumulate
// as overlapping runs (real tiering) or whether the level remains
// non-overlapping regardless of the "tiered" tag (cosmetic tiering).
//
// Public API only. Writes an overwrite-heavy workload, periodically
// inspecting "leveldb.sstables" (what any real consumer of the DB would
// see) for overlapping user-key ranges at level >= 1 -- checked DURING
// the write phase, not just after settling. AMETHYST Phase 3's
// accumulate-then-batch-merge design is inherently cyclic: a tiered
// level fills up with overlapping runs and then fully drains in one
// flatten-down, so the fully-settled, idle end state is *expected* to
// often show zero or few runs even when accumulation is genuinely
// happening -- catching it requires sampling while writes are still
// landing, not only the final resting state.
//
// Sampling-frequency caveat: the during-write check is a snapshot taken
// every kSampleEvery ops (4th CLI arg, default 5000), not a continuous
// observation -- verified this does NOT perturb compaction timing
// (wall-clock time was flat across kSampleEvery in {1000, 5000, 20000,
// never}, ~13-15s regardless), but a COARSE sampling interval can still
// report COSMETIC purely from aliasing: with few samples, missing every
// transient overlap window in the accumulate/drain cycle by bad luck is
// statistically plausible, not evidence the mechanism is inactive. If a
// run reports COSMETIC, prefer more samples (lower kSampleEvery) or more
// writes over concluding accumulation isn't happening.
//
// Usage: ./overlap_check [dbpath] [keyspace] [numwrites] [sampleEvery]

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/options.h"

namespace {

struct FileRange {
  uint64_t number;
  uint64_t file_size;
  std::string smallest;
  std::string largest;
};

// Parses the text produced by Version::DebugString() (exposed via the
// "leveldb.sstables" property). Format per line:
//   --- level N ---
//    <number>:<file_size>['<smallest>' @ <seq> : <type> .. '<largest>' @ <seq> : <type>]
std::map<int, std::vector<FileRange>> ParseSSTables(const std::string& text) {
  std::map<int, std::vector<FileRange>> levels;
  int current_level = -1;
  size_t pos = 0;
  while (pos < text.size()) {
    size_t eol = text.find('\n', pos);
    if (eol == std::string::npos) eol = text.size();
    std::string line = text.substr(pos, eol - pos);
    pos = eol + 1;

    // Trim leading spaces for classification, but keep original for parsing.
    size_t first_non_space = line.find_first_not_of(' ');
    if (first_non_space == std::string::npos) continue;  // blank line

    if (line.compare(first_non_space, 5, "--- l") == 0) {
      int lvl = -1;
      if (std::sscanf(line.c_str() + first_non_space, "--- level %d ---",
                       &lvl) == 1) {
        current_level = lvl;
      }
      continue;
    }

    if (current_level < 0) continue;

    // "<number>:<file_size>[...]"
    size_t colon = line.find(':', first_non_space);
    size_t bracket = line.find('[', colon == std::string::npos ? first_non_space : colon);
    if (colon == std::string::npos || bracket == std::string::npos) continue;

    FileRange fr;
    fr.number = std::strtoull(line.substr(first_non_space, colon - first_non_space).c_str(), nullptr, 10);
    fr.file_size = std::strtoull(line.substr(colon + 1, bracket - colon - 1).c_str(), nullptr, 10);

    size_t q1 = line.find('\'', bracket);
    if (q1 == std::string::npos) continue;
    size_t q2 = line.find('\'', q1 + 1);
    if (q2 == std::string::npos) continue;
    fr.smallest = line.substr(q1 + 1, q2 - q1 - 1);

    size_t dotdot = line.find(" .. ", q2);
    if (dotdot == std::string::npos) continue;
    size_t q3 = line.find('\'', dotdot);
    if (q3 == std::string::npos) continue;
    size_t q4 = line.find('\'', q3 + 1);
    if (q4 == std::string::npos) continue;
    fr.largest = line.substr(q3 + 1, q4 - q3 - 1);

    levels[current_level].push_back(fr);
  }
  return levels;
}

std::string ZeroPad(int value, int width) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%0*d", width, value);
  return std::string(buf);
}

// Returns true (and prints details) iff any level >= 1 has overlapping
// file ranges in `levels`.
bool CheckOverlap(const std::map<int, std::vector<FileRange>>& levels,
                  const char* label) {
  bool any_overlap = false;
  for (auto& kv : levels) {
    int level = kv.first;
    const auto& files = kv.second;
    if (level < 1) continue;  // L0 is expected to overlap; not relevant here.

    std::vector<FileRange> sorted_files = files;
    std::sort(sorted_files.begin(), sorted_files.end(),
              [](const FileRange& a, const FileRange& b) {
                return a.smallest < b.smallest;
              });

    bool have_max = false;
    std::string running_max;
    for (const auto& f : sorted_files) {
      if (have_max && f.smallest <= running_max) {
        any_overlap = true;
        std::printf(
            "  [%s] OVERLAP at level %d: file %llu [%s..%s] overlaps "
            "running max %s\n",
            label, level, static_cast<unsigned long long>(f.number),
            f.smallest.c_str(), f.largest.c_str(), running_max.c_str());
      }
      if (!have_max || f.largest > running_max) {
        running_max = f.largest;
        have_max = true;
      }
    }
  }
  return any_overlap;
}

}  // namespace

int main(int argc, char** argv) {
  std::string dbname = argc > 1 ? argv[1] : "/tmp/claude-1000/-home-suchitra-suchi-personal-leveldb-fork/4cab33d3-9fa0-405d-ae3e-2c8272dff75f/scratchpad/overlap_check_db";

  leveldb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.filter_policy = nullptr;
  options.compression = leveldb::kNoCompression;
  options.write_buffer_size = 1 * 1024 * 1024;   // 1MB
  options.max_file_size = 512 * 1024;            // 512KB
  // AMETHYST: adaptive_enabled defaults to false as of Phase 3 (every
  // FileMetaData defaults to kTiered, so leaving this on by default would
  // make every DB in this fork -- not just ones asking for Amethyst --
  // accumulate). This diagnostic exists specifically to check the
  // adaptive engine's compaction behavior, so it opts in explicitly.
  options.adaptive_enabled = true;

  leveldb::DestroyDB(dbname, options);

  leveldb::DB* db = nullptr;
  leveldb::Status s = leveldb::DB::Open(options, dbname, &db);
  if (!s.ok()) {
    std::fprintf(stderr, "Open failed: %s\n", s.ToString().c_str());
    return 2;
  }

  const int kKeyspace = argc > 2 ? std::atoi(argv[2]) : 4000;
  const int kNumWrites = argc > 3 ? std::atoi(argv[3]) : 300000;
  const int kValueSize = 200;
  // Configurable so the sampling frequency itself can be varied to check
  // it isn't perturbing compaction timing enough to affect the
  // measurement (each sample takes the DB mutex and builds a full
  // DebugString of the current version).
  const int kSampleEvery = argc > 4 ? std::atoi(argv[4]) : 5000;

  std::mt19937 rng(12345);
  std::uniform_int_distribution<int> key_dist(0, kKeyspace - 1);
  std::string filler(kValueSize, 'x');

  bool overlap_seen_during_writes = false;
  int overlap_sample_count = 0;

  leveldb::WriteOptions wopts;
  for (int i = 0; i < kNumWrites; i++) {
    int k = key_dist(rng);
    std::string key = "key" + ZeroPad(k, 7);
    std::string value = std::to_string(i) + filler;
    s = db->Put(wopts, key, value);
    if (!s.ok()) {
      std::fprintf(stderr, "Put failed at i=%d: %s\n", i, s.ToString().c_str());
      return 2;
    }

    if (i > 0 && i % kSampleEvery == 0) {
      std::string sample_prop;
      db->GetProperty("leveldb.sstables", &sample_prop);
      auto sample_levels = ParseSSTables(sample_prop);
      if (CheckOverlap(sample_levels, "during-write")) {
        overlap_seen_during_writes = true;
        overlap_sample_count++;
      }
    }
  }
  std::printf("Wrote %d entries over a %d-key keyspace.\n", kNumWrites, kKeyspace);
  std::printf(
      "During-write sampling: overlap observed in %d of %d samples.\n",
      overlap_sample_count, kNumWrites / kSampleEvery);

  // Let background compaction settle (do NOT call CompactRange, which would
  // force a full merge and hide the natural layout).
  auto total_files = [&]() -> long {
    long total = 0;
    for (int level = 0; level < 7; level++) {
      std::string prop = "leveldb.num-files-at-level" + std::to_string(level);
      std::string val;
      if (db->GetProperty(prop, &val)) {
        total += std::strtol(val.c_str(), nullptr, 10);
      }
    }
    return total;
  };

  long prev = -1;
  int stable_polls = 0;
  const int kMaxPolls = 60;  // ~30s max
  for (int poll = 0; poll < kMaxPolls && stable_polls < 3; poll++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    long cur = total_files();
    if (cur == prev) {
      stable_polls++;
    } else {
      stable_polls = 0;
    }
    prev = cur;
  }
  std::printf("Compaction settle: total sstable count stabilized at %ld.\n", prev);

  std::string sstables_prop;
  db->GetProperty("leveldb.sstables", &sstables_prop);

  auto levels = ParseSSTables(sstables_prop);
  for (auto& kv : levels) {
    std::printf("Level %d: %zu file(s)\n", kv.first, kv.second.size());
  }
  bool overlap_at_settle = CheckOverlap(levels, "settled");

  std::printf("\n%s\n", sstables_prop.c_str());

  // AMETHYST Phase 3's accumulate-then-batch-merge design is cyclic: the
  // fully-settled, idle end state can legitimately show zero overlap
  // even when accumulation genuinely happened during the run (a tiered
  // level fills up, then fully drains in one flatten-down). The verdict
  // is REAL if overlap was observed at ANY point -- during writes or at
  // settle -- not just the final resting state.
  bool any_overlap = overlap_seen_during_writes || overlap_at_settle;

  if (any_overlap) {
    std::printf("VERDICT: REAL (overlapping runs observed at L1+: %s%s%s)\n",
                overlap_seen_during_writes ? "during writes" : "",
                overlap_seen_during_writes && overlap_at_settle ? ", " : "",
                overlap_at_settle ? "at settle" : "");
  } else {
    std::printf("VERDICT: COSMETIC (all L1+ levels remain non-overlapping)\n");
  }

  delete db;
  return 0;
}
