// Diagnostic: determine whether tiered SSTables at L1+ actually accumulate
// as overlapping runs (real tiering) or whether the level remains
// non-overlapping regardless of the "tiered" tag (cosmetic tiering).
//
// Public API only. Writes an overwrite-heavy workload, lets background
// compaction settle, then inspects the "leveldb.sstables" property (which
// is what any real consumer of the DB would see) for overlapping user-key
// ranges at level >= 1.
//
// Usage: ./overlap_check [dbpath]

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

  std::mt19937 rng(12345);
  std::uniform_int_distribution<int> key_dist(0, kKeyspace - 1);
  std::string filler(kValueSize, 'x');

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
  }
  std::printf("Wrote %d entries over a %d-key keyspace.\n", kNumWrites, kKeyspace);

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

  bool any_overlap = false;
  for (auto& kv : levels) {
    int level = kv.first;
    auto& files = kv.second;
    std::printf("Level %d: %zu file(s)\n", level, files.size());
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
            "  OVERLAP at level %d: file %llu [%s..%s] overlaps running max "
            "%s\n",
            level, static_cast<unsigned long long>(f.number), f.smallest.c_str(),
            f.largest.c_str(), running_max.c_str());
      }
      if (!have_max || f.largest > running_max) {
        running_max = f.largest;
        have_max = true;
      }
    }
  }

  std::printf("\n%s\n", sstables_prop.c_str());

  if (any_overlap) {
    std::printf("VERDICT: REAL (overlapping runs observed at L1+)\n");
  } else {
    std::printf("VERDICT: COSMETIC (all L1+ levels remain non-overlapping)\n");
  }

  delete db;
  return 0;
}
