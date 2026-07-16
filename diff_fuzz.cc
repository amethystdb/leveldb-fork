// Diagnostic: randomized differential fuzz test against a std::map shadow.
// Detects stale/incorrect reads (e.g. an older overlapping tiered file
// shadowing a newer one). Every written value embeds an incrementing
// version counter so staleness is directly observable, not just
// presence/absence.
//
// Public API only. PASS/FAIL via exit code. Note: this only exercises the
// tiered overlapping-run resolution path if overlap_check reports REAL
// tiering; against a COSMETIC implementation it mostly re-validates
// standard LevelDB read-path correctness.
//
// Usage: ./diff_fuzz [dbpath] [seed]

#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <string>

#include "leveldb/db.h"
#include "leveldb/options.h"

namespace {

std::string ZeroPad(int value, int width) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%0*d", width, value);
  return std::string(buf);
}

// Value format: "<12-digit version>:" followed by filler to a fixed size.
std::string MakeValue(long long version, int total_size) {
  char prefix[16];
  std::snprintf(prefix, sizeof(prefix), "%012lld:", version);
  std::string v(prefix);
  if (static_cast<int>(v.size()) < total_size) {
    v.append(total_size - v.size(), 'x');
  }
  return v;
}

// Returns true and fills *version if value has the expected format.
bool ParseVersion(const std::string& value, long long* version) {
  if (value.size() < 13 || value[12] != ':') return false;
  *version = std::strtoll(value.substr(0, 12).c_str(), nullptr, 10);
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  std::string dbname = argc > 1 ? argv[1] : "/tmp/claude-1000/-home-suchitra-suchi-personal-leveldb-fork/4cab33d3-9fa0-405d-ae3e-2c8272dff75f/scratchpad/diff_fuzz_db";
  unsigned seed = argc > 2 ? static_cast<unsigned>(std::strtoul(argv[2], nullptr, 10)) : 42u;

  leveldb::Options options;
  options.create_if_missing = true;
  options.filter_policy = nullptr;
  options.compression = leveldb::kNoCompression;
  options.write_buffer_size = 1 * 1024 * 1024;  // 1MB
  options.max_file_size = 512 * 1024;           // 512KB

  leveldb::DestroyDB(dbname, options);

  leveldb::DB* db = nullptr;
  leveldb::Status s = leveldb::DB::Open(options, dbname, &db);
  if (!s.ok()) {
    std::fprintf(stderr, "Open failed: %s\n", s.ToString().c_str());
    return 2;
  }

  const int kKeyspace = 3000;
  const int kNumOps = 200000;
  const int kValueSize = 150;
  const int kCheckEvery = 5;

  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> key_dist(0, kKeyspace - 1);
  std::uniform_int_distribution<int> op_dist(0, 99);

  // ground truth: key -> version if present, absent/erased means deleted.
  std::map<std::string, long long> shadow;

  long long version_counter = 0;
  long long checks = 0;
  long long mismatches = 0;
  const int kMaxPrinted = 20;

  leveldb::WriteOptions wopts;
  leveldb::ReadOptions ropts;

  auto check_key = [&](const std::string& key) {
    std::string value;
    leveldb::Status gs = db->Get(ropts, key, &value);
    checks++;

    auto it = shadow.find(key);
    bool expect_present = (it != shadow.end());

    if (!expect_present) {
      if (!gs.IsNotFound()) {
        mismatches++;
        if (mismatches <= kMaxPrinted) {
          std::fprintf(stderr,
                       "MISMATCH key=%s expected=NotFound actual=%s\n",
                       key.c_str(), gs.ToString().c_str());
        }
      }
      return;
    }

    if (!gs.ok()) {
      mismatches++;
      if (mismatches <= kMaxPrinted) {
        std::fprintf(stderr,
                     "MISMATCH key=%s expected=Found(version=%lld) actual=%s\n",
                     key.c_str(), it->second, gs.ToString().c_str());
      }
      return;
    }

    long long got_version = -1;
    bool parsed = ParseVersion(value, &got_version);
    if (!parsed || got_version != it->second) {
      mismatches++;
      if (mismatches <= kMaxPrinted) {
        std::fprintf(stderr,
                     "MISMATCH key=%s expected_version=%lld actual_version=%lld "
                     "(parsed=%d) -- STALE READ\n",
                     key.c_str(), it->second, got_version, parsed);
      }
    }
  };

  for (int i = 0; i < kNumOps; i++) {
    int k = key_dist(rng);
    std::string key = "key" + ZeroPad(k, 7);

    if (op_dist(rng) < 80) {
      version_counter++;
      std::string value = MakeValue(version_counter, kValueSize);
      s = db->Put(wopts, key, value);
      if (!s.ok()) {
        std::fprintf(stderr, "Put failed at i=%d: %s\n", i, s.ToString().c_str());
        return 2;
      }
      shadow[key] = version_counter;
    } else {
      s = db->Delete(wopts, key);
      if (!s.ok()) {
        std::fprintf(stderr, "Delete failed at i=%d: %s\n", i, s.ToString().c_str());
        return 2;
      }
      shadow.erase(key);
    }

    if (i % kCheckEvery == 0) {
      int ck = key_dist(rng);
      check_key("key" + ZeroPad(ck, 7));
    }
  }

  std::printf("Randomized loop done: %d ops, %lld checks so far, %lld mismatches so far.\n",
              kNumOps, checks, mismatches);

  // Final full sweep over the entire bounded keyspace.
  for (int k = 0; k < kKeyspace; k++) {
    check_key("key" + ZeroPad(k, 7));
  }

  std::printf("Final: checks=%lld mismatches=%lld\n", checks, mismatches);

  delete db;

  if (mismatches > 0) {
    std::printf("RESULT: FAIL\n");
    return 1;
  }
  std::printf("RESULT: PASS\n");
  return 0;
}
