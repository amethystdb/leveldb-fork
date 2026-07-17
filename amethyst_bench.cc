// Amethyst Phase 2: stock-vs-adaptive benchmark harness.
//
// Metrics are matched to the Go prototype's definitions
// (ws-impl/amethyst/cmd/amethystd/main.go):
//   WA = leveldb.bytes-written (flush + compaction output) / user bytes
//        (sum of key+value bytes over every Put issued, non-deduplicated)
//   RA = sum, per Get, of "files across ALL levels whose user-key range
//        contains the queried key" / total Gets -- a live metadata
//        range-overlap count (matching GetSegmentsForKey), NOT "files
//        actually opened via I/O" (which the read path may short-circuit
//        before touching).
//   SA = on-disk .ldb bytes / logical live bytes (sum of key+value size
//        for the current set of live/unique keys). See the reclamation
//        caveat in the accompanying report -- LevelDB eagerly deletes
//        obsolete files, so this number is not comparable across engines
//        that defer/never reclaim.
//
// "adaptive" toggles Options::adaptive_enabled; "stock" in the resulting
// table means this same binary/build with adaptive_enabled=false, which
// Phase 2's parity check (parity_workload.cc) verified is byte-identical
// to a genuine stock LevelDB build on a fixed seed.
//
// Public API only. Usage: ./amethyst_bench <workload> <adaptive:0|1> <dbpath>
//   workload in {fillrandom, readrandom, overwrite, shifting}

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/options.h"

using namespace leveldb;

namespace {

std::string ZeroPad(int value, int width) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%0*d", width, value);
  return std::string(buf);
}

std::string MakeKey(int k) { return "key" + ZeroPad(k, 8); }

struct FileRange {
  std::string smallest;
  std::string largest;
};

// Parses Version::DebugString() (the "leveldb.sstables" property).
std::map<int, std::vector<FileRange>> ParseSSTables(const std::string& text) {
  std::map<int, std::vector<FileRange>> levels;
  int current_level = -1;
  size_t pos = 0;
  while (pos < text.size()) {
    size_t eol = text.find('\n', pos);
    if (eol == std::string::npos) eol = text.size();
    std::string line = text.substr(pos, eol - pos);
    pos = eol + 1;

    size_t first_non_space = line.find_first_not_of(' ');
    if (first_non_space == std::string::npos) continue;

    if (line.compare(first_non_space, 5, "--- l") == 0) {
      int lvl = -1;
      if (std::sscanf(line.c_str() + first_non_space, "--- level %d ---",
                       &lvl) == 1) {
        current_level = lvl;
      }
      continue;
    }
    if (current_level < 0) continue;

    size_t colon = line.find(':', first_non_space);
    size_t bracket = line.find('[', colon == std::string::npos ? first_non_space : colon);
    if (colon == std::string::npos || bracket == std::string::npos) continue;

    size_t q1 = line.find('\'', bracket);
    if (q1 == std::string::npos) continue;
    size_t q2 = line.find('\'', q1 + 1);
    if (q2 == std::string::npos) continue;
    std::string smallest = line.substr(q1 + 1, q2 - q1 - 1);

    size_t dotdot = line.find(" .. ", q2);
    if (dotdot == std::string::npos) continue;
    size_t q3 = line.find('\'', dotdot);
    if (q3 == std::string::npos) continue;
    size_t q4 = line.find('\'', q3 + 1);
    if (q4 == std::string::npos) continue;
    std::string largest = line.substr(q3 + 1, q4 - q3 - 1);

    levels[current_level].push_back({smallest, largest});
  }
  return levels;
}

int64_t CountCandidates(const std::map<int, std::vector<FileRange>>& snapshot,
                        const std::string& key) {
  int64_t count = 0;
  for (const auto& kv : snapshot) {
    for (const auto& fr : kv.second) {
      if (key >= fr.smallest && key <= fr.largest) count++;
    }
  }
  return count;
}

int64_t DirLdbBytes(Env* env, const std::string& dbname) {
  std::vector<std::string> children;
  env->GetChildren(dbname, &children);
  int64_t total = 0;
  for (const auto& c : children) {
    if (c.size() > 4 && c.compare(c.size() - 4, 4, ".ldb") == 0) {
      uint64_t sz = 0;
      if (env->GetFileSize(dbname + "/" + c, &sz).ok()) {
        total += static_cast<int64_t>(sz);
      }
    }
  }
  return total;
}

void SettleCompaction(DB* db) {
  long prev = -1;
  int stable_polls = 0;
  for (int poll = 0; poll < 60 && stable_polls < 3; poll++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    long total = 0;
    for (int level = 0; level < 7; level++) {
      std::string val;
      if (db->GetProperty("leveldb.num-files-at-level" + std::to_string(level),
                          &val)) {
        total += std::strtol(val.c_str(), nullptr, 10);
      }
    }
    stable_polls = (total == prev) ? stable_polls + 1 : 0;
    prev = total;
  }
}

int64_t GetBytesWritten(DB* db) {
  std::string val;
  db->GetProperty("leveldb.bytes-written", &val);
  return std::strtoll(val.c_str(), nullptr, 10);
}

std::string GetStrategyCounts(DB* db) {
  std::string val;
  db->GetProperty("leveldb.strategy-counts", &val);
  return val;
}

// Issues `count` Puts with keys drawn uniformly from [0, keyspace),
// tracking user bytes (key+value size, every Put counted, matching the
// Go prototype's non-deduplicated userBytes) and the live-key map used
// for SA's logical-bytes denominator.
void WritePhase(DB* db, std::mt19937* rng, int keyspace, int count,
                int value_size, int64_t* user_bytes, int* version_counter,
                std::unordered_map<std::string, size_t>* live_keys) {
  std::uniform_int_distribution<int> key_dist(0, keyspace - 1);
  WriteOptions wopts;
  for (int i = 0; i < count; i++) {
    std::string key = MakeKey(key_dist(*rng));
    std::string value =
        std::to_string((*version_counter)++) + std::string(value_size, 'x');
    db->Put(wopts, key, value);
    *user_bytes += static_cast<int64_t>(key.size() + value.size());
    (*live_keys)[key] = value.size();
  }
}

// Timed, uninstrumented read pass: pure Gets, for clean throughput.
// Records the exact key sequence so MeasureRA can faithfully replay it.
double TimedReadPhase(DB* db, std::mt19937* rng, int keyspace, int count,
                      std::vector<std::string>* key_sequence) {
  std::uniform_int_distribution<int> key_dist(0, keyspace - 1);
  key_sequence->clear();
  key_sequence->reserve(count);
  for (int i = 0; i < count; i++) {
    key_sequence->push_back(MakeKey(key_dist(*rng)));
  }

  ReadOptions ropts;
  std::string value;
  auto start = std::chrono::steady_clock::now();
  for (const auto& key : *key_sequence) {
    db->Get(ropts, key, &value);
  }
  auto end = std::chrono::steady_clock::now();
  return std::chrono::duration<double>(end - start).count();
}

// Untimed RA pass: replays the same key sequence, refreshing the SSTable
// range snapshot before every single read (live, matching the Go
// prototype's per-call GetSegmentsForKey), summing range-overlap
// candidates across all levels.
int64_t MeasureRA(DB* db, const std::vector<std::string>& key_sequence) {
  int64_t total_candidates = 0;
  for (const auto& key : key_sequence) {
    std::string sstables_text;
    db->GetProperty("leveldb.sstables", &sstables_text);
    auto snapshot = ParseSSTables(sstables_text);
    total_candidates += CountCandidates(snapshot, key);
  }
  return total_candidates;
}

double LogicalLiveBytes(const std::unordered_map<std::string, size_t>& live_keys) {
  int64_t total = 0;
  for (const auto& kv : live_keys) {
    total += static_cast<int64_t>(kv.first.size() + kv.second);
  }
  return static_cast<double>(total);
}

struct Result {
  std::string workload;
  std::string mode;
  double wa = -1;
  double ra = -1;
  double sa = -1;
  double throughput = -1;
  std::string throughput_label;
  std::string notes;
};

std::string Fmt(double v) {
  if (v < 0) return "n/a";
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%.4f", v);
  return buf;
}

void PrintResult(const Result& r) {
  std::printf("RESULT workload=%s mode=%s WA=%s RA=%s SA=%s throughput=%s %s",
              r.workload.c_str(), r.mode.c_str(), Fmt(r.wa).c_str(),
              Fmt(r.ra).c_str(), Fmt(r.sa).c_str(), Fmt(r.throughput).c_str(),
              r.throughput_label.c_str());
  if (!r.notes.empty()) {
    std::printf(" notes=[%s]", r.notes.c_str());
  }
  std::printf("\n");
}

Options MakeOptions(bool adaptive) {
  Options options;
  options.create_if_missing = true;
  options.filter_policy = nullptr;
  options.compression = kNoCompression;
  options.write_buffer_size = 1 * 1024 * 1024;  // 1MB
  options.max_file_size = 512 * 1024;           // 512KB
  options.adaptive_enabled = adaptive;
  return options;
}

Result RunFillRandom(Env* env, bool adaptive, const std::string& dbname) {
  Options options = MakeOptions(adaptive);
  DestroyDB(dbname, options);
  DB* db = nullptr;
  DB::Open(options, dbname, &db);

  std::mt19937 rng(42);
  int64_t user_bytes = 0;
  int version_counter = 0;
  std::unordered_map<std::string, size_t> live_keys;
  const int kKeyspace = 100000;
  const int kValueSize = 150;

  auto start = std::chrono::steady_clock::now();
  WritePhase(db, &rng, kKeyspace, kKeyspace, kValueSize, &user_bytes,
            &version_counter, &live_keys);
  auto end = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double>(end - start).count();

  SettleCompaction(db);

  Result r;
  r.workload = "fillrandom";
  r.mode = adaptive ? "adaptive" : "stock";
  r.wa = user_bytes > 0 ? double(GetBytesWritten(db)) / user_bytes : 0.0;
  r.sa = double(DirLdbBytes(env, dbname)) / LogicalLiveBytes(live_keys);
  r.throughput = kKeyspace / elapsed;
  r.throughput_label = "writes/s";
  r.notes = GetStrategyCounts(db);

  delete db;
  return r;
}

Result RunReadRandom(Env* env, bool adaptive, const std::string& dbname) {
  Options options = MakeOptions(adaptive);
  DestroyDB(dbname, options);
  DB* db = nullptr;
  DB::Open(options, dbname, &db);

  std::mt19937 rng(43);
  int64_t user_bytes = 0;
  int version_counter = 0;
  std::unordered_map<std::string, size_t> live_keys;
  const int kKeyspace = 100000;
  const int kValueSize = 150;
  const int kReads = 30000;

  // Setup (untimed): populate, then let compaction settle.
  WritePhase(db, &rng, kKeyspace, kKeyspace, kValueSize, &user_bytes,
            &version_counter, &live_keys);
  SettleCompaction(db);

  std::mt19937 read_rng(44);
  std::vector<std::string> key_sequence;
  double elapsed = TimedReadPhase(db, &read_rng, kKeyspace, kReads, &key_sequence);
  int64_t total_candidates = MeasureRA(db, key_sequence);

  Result r;
  r.workload = "readrandom";
  r.mode = adaptive ? "adaptive" : "stock";
  r.wa = user_bytes > 0 ? double(GetBytesWritten(db)) / user_bytes : 0.0;
  r.ra = double(total_candidates) / kReads;
  r.sa = double(DirLdbBytes(env, dbname)) / LogicalLiveBytes(live_keys);
  r.throughput = kReads / elapsed;
  r.throughput_label = "reads/s";
  r.notes = "WA/SA include the populate phase; " + GetStrategyCounts(db);

  delete db;
  return r;
}

Result RunOverwrite(Env* env, bool adaptive, const std::string& dbname) {
  Options options = MakeOptions(adaptive);
  DestroyDB(dbname, options);
  DB* db = nullptr;
  DB::Open(options, dbname, &db);

  std::mt19937 rng(45);
  int64_t user_bytes = 0;
  int version_counter = 0;
  std::unordered_map<std::string, size_t> live_keys;
  const int kKeyspace = 3000;  // small, overwrite-heavy
  const int kValueSize = 150;
  const int kOverwrites = 200000;

  // Setup (untimed): initial population of the small keyspace.
  WritePhase(db, &rng, kKeyspace, kKeyspace, kValueSize, &user_bytes,
            &version_counter, &live_keys);

  auto start = std::chrono::steady_clock::now();
  WritePhase(db, &rng, kKeyspace, kOverwrites, kValueSize, &user_bytes,
            &version_counter, &live_keys);
  auto end = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double>(end - start).count();

  SettleCompaction(db);

  Result r;
  r.workload = "overwrite";
  r.mode = adaptive ? "adaptive" : "stock";
  r.wa = user_bytes > 0 ? double(GetBytesWritten(db)) / user_bytes : 0.0;
  r.sa = double(DirLdbBytes(env, dbname)) / LogicalLiveBytes(live_keys);
  r.throughput = kOverwrites / elapsed;
  r.throughput_label = "writes/s";
  r.notes = "WA/SA include the populate phase; " + GetStrategyCounts(db);

  delete db;
  return r;
}

Result RunShifting(Env* env, bool adaptive, const std::string& dbname) {
  Options options = MakeOptions(adaptive);
  DestroyDB(dbname, options);
  DB* db = nullptr;
  DB::Open(options, dbname, &db);

  std::mt19937 rng(46);
  int64_t user_bytes = 0;
  int version_counter = 0;
  std::unordered_map<std::string, size_t> live_keys;
  const int kKeyspace = 5000;
  const int kValueSize = 150;
  const int kPhaseOps = 8000;
  const int kCycles = 3;

  WritePhase(db, &rng, kKeyspace, kKeyspace, kValueSize, &user_bytes,
            &version_counter, &live_keys);
  SettleCompaction(db);

  double total_read_secs = 0, total_write_secs = 0;
  int64_t total_reads = 0, total_writes = 0;
  int64_t total_candidates = 0;
  std::string strategy_progression;

  for (int cycle = 0; cycle < kCycles; cycle++) {
    // Read-heavy phase: pure reads, exactly the "no write pressure"
    // scenario the periodic poll (Phase 1a-extra) exists to handle.
    std::mt19937 read_rng(1000 + cycle);
    std::vector<std::string> key_sequence;
    total_read_secs +=
        TimedReadPhase(db, &read_rng, kKeyspace, kPhaseOps, &key_sequence);
    total_reads += kPhaseOps;
    total_candidates += MeasureRA(db, key_sequence);

    strategy_progression += "[cycle " + std::to_string(cycle) +
                            " after read phase: " + GetStrategyCounts(db) + "] ";

    // Write-heavy phase: pure overwrites over the same keyspace.
    auto start = std::chrono::steady_clock::now();
    WritePhase(db, &rng, kKeyspace, kPhaseOps, kValueSize, &user_bytes,
              &version_counter, &live_keys);
    auto end = std::chrono::steady_clock::now();
    total_write_secs += std::chrono::duration<double>(end - start).count();
    total_writes += kPhaseOps;
  }

  SettleCompaction(db);

  Result r;
  r.workload = "shifting";
  r.mode = adaptive ? "adaptive" : "stock";
  r.wa = user_bytes > 0 ? double(GetBytesWritten(db)) / user_bytes : 0.0;
  r.ra = double(total_candidates) / total_reads;
  r.sa = double(DirLdbBytes(env, dbname)) / LogicalLiveBytes(live_keys);
  r.throughput = total_reads / total_read_secs;  // reads/s; write throughput in notes
  r.throughput_label = "reads/s (read phases)";
  char write_tp[64];
  std::snprintf(write_tp, sizeof(write_tp), "%.0f", total_writes / total_write_secs);
  r.notes = std::string("write throughput=") + write_tp + " writes/s; " +
           strategy_progression + "final: " + GetStrategyCounts(db);

  delete db;
  return r;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 4) {
    std::fprintf(stderr,
                 "usage: %s <fillrandom|readrandom|overwrite|shifting> "
                 "<adaptive:0|1> <dbpath>\n",
                 argv[0]);
    return 2;
  }
  std::string workload = argv[1];
  bool adaptive = std::atoi(argv[2]) != 0;
  std::string dbname = argv[3];
  Env* env = Env::Default();

  Result r;
  if (workload == "fillrandom") {
    r = RunFillRandom(env, adaptive, dbname);
  } else if (workload == "readrandom") {
    r = RunReadRandom(env, adaptive, dbname);
  } else if (workload == "overwrite") {
    r = RunOverwrite(env, adaptive, dbname);
  } else if (workload == "shifting") {
    r = RunShifting(env, adaptive, dbname);
  } else {
    std::fprintf(stderr, "unknown workload: %s\n", workload.c_str());
    return 2;
  }
  PrintResult(r);
  return 0;
}
