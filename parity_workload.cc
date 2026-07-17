// Amethyst Phase 2: verifies that adaptive_enabled=false is byte-identical
// to stock LevelDB. Compiled twice against two different trees:
//   - with -DLEVELDB_AMETHYST, linked against this fork's libleveldb.a,
//     with Options::adaptive_enabled explicitly set to false.
//   - without the define, linked against a stock LevelDB build (this
//     option doesn't exist there, so the line is compiled out).
// Both binaries run the identical fixed-seed workload against a fresh DB
// directory; a separate comparison step (see verify_parity.sh) diffs the
// resulting .ldb files byte-for-byte.
//
// Public API only.

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <thread>

#include "leveldb/db.h"
#include "leveldb/options.h"

namespace {

std::string ZeroPad(int value, int width) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%0*d", width, value);
  return std::string(buf);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s <dbpath>\n", argv[0]);
    return 2;
  }
  std::string dbname = argv[1];

  leveldb::Options options;
  options.create_if_missing = true;
  options.filter_policy = nullptr;
  options.compression = leveldb::kNoCompression;
  options.write_buffer_size = 256 * 1024;
  options.max_file_size = 128 * 1024;
#ifdef LEVELDB_AMETHYST
  options.adaptive_enabled = false;
#endif

  leveldb::DestroyDB(dbname, options);
  leveldb::DB* db = nullptr;
  leveldb::Status s = leveldb::DB::Open(options, dbname, &db);
  if (!s.ok()) {
    std::fprintf(stderr, "Open failed: %s\n", s.ToString().c_str());
    return 2;
  }

  const int kKeyspace = 3000;
  const int kNumOps = 150000;
  const int kValueSize = 150;

  std::mt19937 rng(20260718);
  std::uniform_int_distribution<int> key_dist(0, kKeyspace - 1);
  std::uniform_int_distribution<int> op_dist(0, 99);
  std::string filler(kValueSize, 'x');

  leveldb::WriteOptions wopts;
  for (int i = 0; i < kNumOps; i++) {
    int k = key_dist(rng);
    std::string key = "key" + ZeroPad(k, 7);
    if (op_dist(rng) < 80) {
      std::string value = std::to_string(i) + filler;
      s = db->Put(wopts, key, value);
    } else {
      s = db->Delete(wopts, key);
    }
    if (!s.ok()) {
      std::fprintf(stderr, "Write failed at i=%d: %s\n", i, s.ToString().c_str());
      return 2;
    }
  }

  // Let background compaction settle before closing: otherwise shutdown
  // can race an in-flight compaction (DoCompactionWork observes
  // shutting_down_ and aborts mid-output), which is a real but
  // Amethyst-unrelated LevelDB shutdown race, not a byte-identity bug --
  // avoid it here so the comparison reflects steady-state output only.
  long prev = -1;
  int stable_polls = 0;
  for (int poll = 0; poll < 60 && stable_polls < 3; poll++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    long total = 0;
    for (int level = 0; level < 7; level++) {
      std::string val;
      if (db->GetProperty(
              "leveldb.num-files-at-level" + std::to_string(level), &val)) {
        total += std::strtol(val.c_str(), nullptr, 10);
      }
    }
    stable_polls = (total == prev) ? stable_polls + 1 : 0;
    prev = total;
  }

  delete db;
  std::printf("done\n");
  return 0;
}
