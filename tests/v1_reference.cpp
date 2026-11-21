// Shopping Sessions pipeline: a native, multithreaded implementation of rules.md.
//
// Reads  $DATA_DIR    (default /data)
// Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
//
// Phases:
//   1. lookups (manifest, products, campaigns)
//   2. links: parse, parallel union-find, person name = byte-wise smallest ID
//   3. events + clicks: parse every file in parallel chunks.
//        events -> partitioned by hash(event_id)   (for dedup)
//        clicks -> validated, mapped to person, partitioned by hash(person)
//   4. per event_id partition: dedup (smallest ingest_ts), keep valid ones,
//        map to person, re-partition by hash(person)
//   5. per person partition: sort, sessionize, attribute, aggregate
//   6. write Parquet
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;
typedef int64_t i64;
typedef uint64_t u64;
typedef uint32_t u32;
typedef int32_t i32;
typedef uint16_t u16;
typedef uint8_t u8;

static const i64 DAY_MS = 86400000LL;
static const i64 GAP_MS = 1800000LL;
static const i64 WEEK_MS = 7 * DAY_MS;

static int NT = 8;
static const int PB = 10;
static const int NP = 1 << PB;
static bool DBG = false;
static chrono::steady_clock::time_point T0;
static void tick(const char* what) {
    if (DBG)
        fprintf(stderr, "%9.3f ms  %s\n",
                chrono::duration<double, milli>(chrono::steady_clock::now() - T0).count(), what);
}
static void die(const char* msg) {
    fprintf(stderr, "fatal: %s\n", msg);
    exit(1);
}

static inline u64 mix64(u64 x) {
    x ^= x >> 31;
    x *= 0x7fb5d329728ea185ULL;
    x ^= x >> 27;
    x *= 0x81dadef4bc2dd44dULL;
    x ^= x >> 33;
    return x;
}
static inline int part_of(u64 k) { return (int)(mix64(k) >> (64 - PB)); }

template <class F>
static void par_for(int n, F&& f) {
    atomic<int> next(0);
    auto work = [&](int tid) {
        for (;;) {
            int i = next.fetch_add(1, memory_order_relaxed);
            if (i >= n) break;
            f(i, tid);
        }
    };
    vector<thread> th;
    for (int t = 1; t < NT; t++) th.emplace_back(work, t);
    work(0);
    for (auto& x : th) x.join();
}

// ---------------------------------------------------------------------------
// Keys: an exact, order-preserving 64-bit code for short IDs.
// Strings of at most 10 chars from [0-9A-Z_a-z] pack 6 bits per char, left
// aligned, so numeric order equals byte-wise order. Anything else is interned
// in a pool and flagged with the top bit. Key 0 is never produced (null).
// ---------------------------------------------------------------------------
static u8 CODE[256];
static char DEC[64];
static void init_codes() {
    int c = 1;
    for (int x = '0'; x <= '9'; x++) CODE[x] = c, DEC[c++] = (char)x;
    for (int x = 'A'; x <= 'Z'; x++) CODE[x] = c, DEC[c++] = (char)x;
    CODE[(int)'_'] = c, DEC[c++] = '_';
    for (int x = 'a'; x <= 'z'; x++) CODE[x] = c, DEC[c++] = (char)x;
}
static const u64 POOLBIT = 1ULL << 63;
static mutex pool_mu;
static unordered_map<string, u64> pool_map;
static vector<string> pool_strs;

static u64 pool_intern(const char* p, size_t n) {
    lock_guard<mutex> g(pool_mu);
    string s(p, n);
    auto it = pool_map.find(s);
    if (it != pool_map.end()) return it->second;
    u64 k = POOLBIT | (u64)pool_strs.size();
    pool_strs.push_back(s);
    pool_map.emplace(std::move(s), k);
    return k;
}
static u64 pool_find(const char* p, size_t n) {
    lock_guard<mutex> g(pool_mu);
    auto it = pool_map.find(string(p, n));
    return it == pool_map.end() ? 0 : it->second;
}
static inline bool short_code(const char* p, size_t n, u64& k) {
    if (n > 10) return false;
    u64 r = 0;
    u32 bad = 0;
    for (size_t i = 0; i < n; i++) {
        u64 c = CODE[(u8)p[i]];
        bad |= (c == 0);
        r |= c << (54 - 6 * i);
    }
    k = r;
    return !bad;
}
static inline u64 key_of(const char* p, size_t n) {
    u64 k;
    if (short_code(p, n, k)) return k;
    return pool_intern(p, n);
}
static inline u64 key_find(const char* p, size_t n) {
    u64 k;
    if (short_code(p, n, k)) return k;
    return pool_find(p, n);
}
static string key_str(u64 k) {
    if (k & POOLBIT) {
        lock_guard<mutex> g(pool_mu);
        return pool_strs[k & ~POOLBIT];
    }
    string s;
    for (int i = 0; i < 10; i++) {
        int c = (int)((k >> (54 - 6 * i)) & 63);
        if (!c) break;
        s.push_back(DEC[c]);
    }
    return s;
}
static inline bool key_less(u64 a, u64 b) {
    if (!((a | b) & POOLBIT)) return a < b;
    return key_str(a) < key_str(b);
}

// ---------------------------------------------------------------------------
// Open-addressing hash map u64 -> u64 (key 0 = empty).
// ---------------------------------------------------------------------------
struct KV {
    u64 k, v;
};
struct FMap {
    vector<KV> t;
    u64 mask = 0;
    void init(size_t n) {
        size_t c = 16;
        while (c < n * 2) c <<= 1;
        t.assign(c, KV{0, 0});
        mask = c - 1;
    }
    KV* slot(u64 k) {
        u64 i = mix64(k) & mask;
        while (t[i].k && t[i].k != k) i = (i + 1) & mask;
        return &t[i];
    }
    const KV* get(u64 k) const {
        u64 i = mix64(k) & mask;
        for (;;) {
            const KV& e = t[i];
            if (e.k == k) return &e;
            if (!e.k) return nullptr;
            i = (i + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Timestamps, prices, CSV dialect
// ---------------------------------------------------------------------------
static inline bool D(char c) { return (u8)(c - '0') < 10; }
static inline int d2(const char* p) { return (p[0] - '0') * 10 + (p[1] - '0'); }
static inline i64 days_from_civil(i64 y, i64 m, i64 d) {
    y -= m <= 2;
    i64 era = (y >= 0 ? y : y - 399) / 400;
    i64 yoe = y - era * 400;
    i64 doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    i64 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}
static inline bool is_leap(i64 y) { return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0; }
static inline i64 floordiv(i64 a, i64 b) { return a / b - ((a % b != 0) && ((a < 0) != (b < 0))); }

// true = a valid timestamp (F1, F2 or F3); the caller checks the window.
static bool parse_ts(const char* p, size_t n, i64& out) {
    if ((n == 20 || n == 25) && p[4] == '-') {
        if (!(D(p[0]) && D(p[1]) && D(p[2]) && D(p[3]) && D(p[5]) && D(p[6]) && p[7] == '-' &&
              D(p[8]) && D(p[9]) && p[10] == 'T' && D(p[11]) && D(p[12]) && p[13] == ':' &&
              D(p[14]) && D(p[15]) && p[16] == ':' && D(p[17]) && D(p[18])))
            return false;
        int y = d2(p) * 100 + d2(p + 2), mo = d2(p + 5), d = d2(p + 8), h = d2(p + 11),
            mi = d2(p + 14), s = d2(p + 17);
        if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59) return false;
        static const int DIM[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int dm = DIM[mo - 1] + (mo == 2 && is_leap(y));
        if (d > dm) return false;
        i64 off = 0;
        if (n == 20) {
            if (p[19] != 'Z') return false;
        } else {
            char sg = p[19];
            if (sg != '+' && sg != '-') return false;
            if (!(D(p[20]) && D(p[21]) && p[22] == ':' && D(p[23]) && D(p[24]))) return false;
            int oh = d2(p + 20), om = d2(p + 23);
            if (oh > 23 || om > 59) return false;
            off = (oh * 60 + om) * 60000LL;
            if (sg == '-') off = -off;
        }
        out = (days_from_civil(y, mo, d) * 86400LL + h * 3600 + mi * 60 + s) * 1000LL - off;
        return true;
    }
    if (n == 0) return false;
    for (size_t j = 0; j < n; j++)
        if (!D(p[j])) return false;
    size_t i = 0;
    while (i < n && p[i] == '0') i++;
    if (n - i > 19) return false;  // beyond 64 bits: outside every window
    u64 v = 0;
    for (; i < n; i++) v = v * 10 + (u64)(p[i] - '0');
    if (v > (u64)INT64_MAX) return false;
    out = (i64)v;
    return true;
}

static inline bool parse_price(const char* p, size_t n, u32& out) {
    if (n == 0) return false;
    for (size_t j = 0; j < n; j++)
        if (!D(p[j])) return false;
    size_t i = 0;
    while (i < n && p[i] == '0') i++;
    if (n - i > 8) return false;
    u32 v = 0;
    for (; i < n; i++) v = v * 10 + (u32)(p[i] - '0');
    if (v < 1 || v > 10000000) return false;
    out = v;
    return true;
}

// Splits [p, e) into exactly NF fields. Quotes may only wrap a whole field.
template <int NF>
static inline bool split(const char* p, const char* e, const char** fs, u32* fl) {
    int f = 0;
    const char* q = p;
    for (;;) {
        if (f >= NF) return false;
        if (q < e && *q == '"') {
            const char* s = q + 1;
            const char* c = (const char*)memchr(s, '"', (size_t)(e - s));
            if (!c) return false;
            fs[f] = s;
            fl[f] = (u32)(c - s);
            f++;
            q = c + 1;
            if (q == e) break;
            if (*q != ',') return false;
            q++;
        } else {
            const char* s = q;
            while (q < e && *q != ',' && *q != '"') q++;
            if (q < e && *q == '"') return false;
            fs[f] = s;
            fl[f] = (u32)(q - s);
            f++;
            if (q == e) break;
            q++;
        }
    }
    return f == NF;
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------
struct Mapped {
    const char* p = nullptr;
    size_t n = 0;
};
static Mapped map_file(const string& path) {
    Mapped m;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return m;
    struct stat st;
    if (fstat(fd, &st) == 0 && st.st_size > 0) {
        void* a = mmap(nullptr, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (a != MAP_FAILED) {
            m.p = (const char*)a;
            m.n = (size_t)st.st_size;
        }
    }
    close(fd);
    return m;
}
static vector<string> list_csv(const string& dir) {
    vector<string> out;
    DIR* d = opendir(dir.c_str());
    if (!d) return out;
    while (dirent* e = readdir(d)) {
        string name = e->d_name;
        if (name.size() < 4 || name[0] == '.' || name.compare(name.size() - 4, 4, ".csv") != 0) continue;
        string path = dir + "/" + name;
        struct stat st;
        if (stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode)) out.push_back(path);
    }
    closedir(d);
    sort(out.begin(), out.end());
    return out;
}

struct Task {
    const char* b;
    size_t size, begin, end;
    int kind;
};
static void add_tasks(vector<Task>& tasks, const vector<Mapped>& files, int kind, size_t chunk) {
    for (auto& m : files) {
        if (!m.n) continue;
        for (size_t s = 0; s < m.n; s += chunk) tasks.push_back(Task{m.p, m.n, s, min(m.n, s + chunk), kind});
    }
}
// Calls f(lineStart, lineEnd) for every data line that starts in [begin, end).
template <class F>
static void for_lines(const Task& t, F&& f) {
    const char* b = t.b;
    size_t pos = t.begin;
    if (pos == 0) {
        const char* nl = (const char*)memchr(b, '\n', t.size);
        if (!nl) return;
        pos = (size_t)(nl - b) + 1;  // skip the header
    } else if (b[pos - 1] != '\n') {
        const char* nl = (const char*)memchr(b + pos, '\n', t.size - pos);
        if (!nl) return;
        pos = (size_t)(nl - b) + 1;
    }
    while (pos < t.end) {
        const char* nl = (const char*)memchr(b + pos, '\n', t.size - pos);
        size_t le = nl ? (size_t)(nl - b) : t.size;
        f(b + pos, b + le);
        pos = le + 1;
    }
}
static string read_small(const string& path) {
    Mapped m = map_file(path);
    string s(m.p ? m.p : "", m.n);
    if (m.p) munmap((void*)m.p, m.n);
    return s;
}
template <class F>
static void for_small_lines(const string& text, F&& f) {
    size_t pos = text.find('\n');
    if (pos == string::npos) return;
    pos++;
    while (pos < text.size()) {
        size_t nl = text.find('\n', pos);
        size_t le = nl == string::npos ? text.size() : nl;
        f(text.data() + pos, text.data() + le);
        pos = le + 1;
    }
}

// ---------------------------------------------------------------------------
// Parquet writer (uncompressed, PLAIN, one row group)
// ---------------------------------------------------------------------------
struct TW {
    string b;
    vector<int> last{0};
    void uv(u64 v) {
        while (v >= 0x80) {
            b.push_back((char)(v | 0x80));
            v >>= 7;
        }
        b.push_back((char)v);
    }
    void zz(i64 v) { uv(((u64)v << 1) ^ (u64)(v >> 63)); }
    void fh(int id, int ty) {
        int d = id - last.back();
        if (d > 0 && d <= 15)
            b.push_back((char)((d << 4) | ty));
        else {
            b.push_back((char)ty);
            zz(id);
        }
        last.back() = id;
    }
    void fi32(int id, i64 v) { fh(id, 5), zz(v); }
    void fi64(int id, i64 v) { fh(id, 6), zz(v); }
    void fstr(int id, const string& s) { fh(id, 8), uv(s.size()), b += s; }
    void fstruct(int id) { fh(id, 12), last.push_back(0); }
    void sbegin() { last.push_back(0); }
    void send() { b.push_back(0), last.pop_back(); }
    void flist(int id, int ety, int n) {
        fh(id, 9);
        if (n < 15)
            b.push_back((char)((n << 4) | ety));
        else {
            b.push_back((char)(0xF0 | ety));
            uv((u64)n);
        }
    }
};
enum { PT_INT32 = 1, PT_INT64 = 2, PT_BYTES = 6 };
struct ColData {
    string name;
    int ptype;
    bool optional;
    int conv;     // -1 none, 0 UTF8, 6 DATE
    int logical;  // 0 none, 1 STRING, 6 DATE
    string page;
};
static void put32(string& s, u32 v) { s.append((const char*)&v, 4); }
static void put64(string& s, u64 v) { s.append((const char*)&v, 8); }

static void write_parquet(const string& path, i64 nrows, const vector<ColData>& cols) {
    string out = "PAR1";
    vector<i64> offs, sizes;
    i64 total = 0;
    if (nrows > 0) {
        for (auto& c : cols) {
            TW h;
            h.fi32(1, 0);
            h.fi32(2, (i64)c.page.size());
            h.fi32(3, (i64)c.page.size());
            h.fstruct(5);
            h.fi32(1, nrows);
            h.fi32(2, 0);
            h.fi32(3, 3);
            h.fi32(4, 3);
            h.send();
            h.b.push_back(0);
            offs.push_back((i64)out.size());
            out += h.b;
            out += c.page;
            sizes.push_back((i64)(h.b.size() + c.page.size()));
            total += sizes.back();
        }
    }
    TW f;
    f.fi32(1, 1);
    f.flist(2, 12, (int)cols.size() + 1);
    f.sbegin();
    f.fstr(4, "schema");
    f.fi32(5, (i64)cols.size());
    f.send();
    for (auto& c : cols) {
        f.sbegin();
        f.fi32(1, c.ptype);
        f.fi32(3, c.optional ? 1 : 0);
        f.fstr(4, c.name);
        if (c.conv >= 0) f.fi32(6, c.conv);
        if (c.logical) {
            f.fstruct(10);
            f.fstruct(c.logical);
            f.send();
            f.send();
        }
        f.send();
    }
    f.fi64(3, nrows);
    if (nrows > 0) {
        f.flist(4, 12, 1);
        f.sbegin();
        f.flist(1, 12, (int)cols.size());
        for (size_t i = 0; i < cols.size(); i++) {
            auto& c = cols[i];
            f.sbegin();
            f.fi64(2, offs[i]);
            f.fstruct(3);
            f.fi32(1, c.ptype);
            if (c.optional) {
                f.flist(2, 5, 2);
                f.zz(0);
                f.zz(3);
            } else {
                f.flist(2, 5, 1);
                f.zz(0);
            }
            f.flist(3, 8, 1);
            f.uv(c.name.size());
            f.b += c.name;
            f.fi32(4, 0);
            f.fi64(5, nrows);
            f.fi64(6, sizes[i]);
            f.fi64(7, sizes[i]);
            f.fi64(9, offs[i]);
            f.send();
            f.send();
        }
        f.fi64(2, total);
        f.fi64(3, nrows);
        f.send();
    } else {
        f.flist(4, 12, 0);
    }
    f.fstr(6, "shopping-sessions");
    f.b.push_back(0);
    out += f.b;
    put32(out, (u32)f.b.size());
    out += "PAR1";
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) die("cannot write output");
    fwrite(out.data(), 1, out.size(), fp);
    fclose(fp);
}
static void page_strings(string& page, const vector<string>& v) {
    for (auto& s : v) put32(page, (u32)s.size()), page += s;
}
static void page_opt_strings(string& page, const vector<const string*>& v) {
    string rle;
    TW t;
    size_t i = 0, n = v.size();
    while (i < n) {
        bool d = v[i] != nullptr;
        size_t j = i;
        while (j < n && (v[j] != nullptr) == d) j++;
        t.b.clear();
        t.uv((u64)(j - i) << 1);
        rle += t.b;
        rle.push_back(d ? 1 : 0);
        i = j;
    }
    put32(page, (u32)rle.size());
    page += rle;
    for (auto* s : v)
        if (s) put32(page, (u32)s->size()), page += *s;
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------
struct EvRec {
    u64 eid;
    i64 ingest;
    u64 user;
    i64 ts;
    u32 price;
    u16 cat;
    u8 flags;  // 1 = valid (kept if this copy wins), 2 = purchase
    u8 pad;
};
struct PRec {
    u64 person;
    i64 ts;
    u32 price;  // purchase price, 0 for view/cart
    u16 cat;
    u8 purch;
    u8 pad;
};
struct CRec {
    u64 person;
    i64 ts;
    u64 cid;
    u32 camp;
    u32 pad;
};

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------
static i64 WS, WE, DAY0;
static int NDAYS;
static FMap PRODUCTS;  // product key -> category index
static vector<string> CATNAMES;  // sorted byte-wise; index = rank
static int UNKNOWN_CAT;
static FMap CAMPAIGNS;  // campaign key -> 1..N
static vector<string> CAMP_ID, CAMP_CH;  // index 0 = organic
static FMap LM[NP];  // linked id -> person name

static inline u64 person_of(u64 k) {
    const KV* e = LM[part_of(k)].get(k);
    return e ? e->v : k;
}

struct Out {
    vector<u64> uid;
    vector<i32> day;
    vector<i64> ses, evs, rev;
    vector<i32> top;
    vector<i64> cs, cps, crev, cas;
    vector<int> cnt, pos, active;
    vector<i64> catsum;
    vector<int> touched;
};

static string env_or(const char* k, const char* d) {
    const char* v = getenv(k);
    return v && *v ? string(v) : string(d);
}

static bool find_json_ts(const string& js, const char* key, i64& out) {
    size_t k = js.find(string("\"") + key + "\"");
    if (k == string::npos) return false;
    size_t c = js.find(':', k);
    size_t q1 = js.find('"', c + 1);
    size_t q2 = js.find('"', q1 + 1);
    if (c == string::npos || q1 == string::npos || q2 == string::npos) return false;
    return parse_ts(js.data() + q1 + 1, q2 - q1 - 1, out);
}

// Concurrent union-find: parents always point to a smaller index.
static inline u32 uf_find(atomic<u32>* par, u32 x) {
    for (;;) {
        u32 p = par[x].load(memory_order_relaxed);
        if (p == x) return x;
        u32 gp = par[p].load(memory_order_relaxed);
        if (gp != p) par[x].compare_exchange_weak(p, gp, memory_order_relaxed);
        x = gp;
    }
}
static inline void uf_union(atomic<u32>* par, u32 a, u32 b) {
    for (;;) {
        a = uf_find(par, a);
        b = uf_find(par, b);
        if (a == b) return;
        if (a < b) swap(a, b);
        u32 exp = a;
        if (par[a].compare_exchange_strong(exp, b)) return;
    }
}

int main(int argc, char** argv) {
    T0 = chrono::steady_clock::now();
    DBG = getenv("SS_DEBUG") != nullptr;
    init_codes();
    string DATA = env_or("DATA_DIR", "/data");
    string OUT = env_or("OUTPUT_DIR", "/output");
    if (argc >= 3) DATA = argv[argc - 2], OUT = argv[argc - 1];
    {
        int n = atoi(env_or("NCPU", "0").c_str());
        if (n <= 0) n = (int)thread::hardware_concurrency();
        NT = max(1, min(n, 64));
    }

    // ---- lookups -----------------------------------------------------------
    {
        string js = read_small(DATA + "/manifest.json");
        if (!find_json_ts(js, "window_start", WS) || !find_json_ts(js, "window_end", WE)) die("bad manifest");
        DAY0 = floordiv(WS, DAY_MS);
        i64 last = WE > WS ? floordiv(WE - 1, DAY_MS) : DAY0;
        NDAYS = (int)max<i64>(1, last - DAY0 + 1);
    }
    {
        string text = read_small(DATA + "/products.csv");
        vector<pair<u64, string>> rows;
        for_small_lines(text, [&](const char* p, const char* e) {
            const char* f[2];
            u32 l[2];
            if (!split<2>(p, e, f, l) || !l[0]) return;
            rows.push_back({key_of(f[0], l[0]), string(f[1], l[1])});
        });
        vector<string> names;
        for (auto& r : rows) names.push_back(r.second);
        names.push_back("unknown");
        sort(names.begin(), names.end());
        names.erase(unique(names.begin(), names.end()), names.end());
        CATNAMES = names;
        unordered_map<string, int> idx;
        for (size_t i = 0; i < names.size(); i++) idx[names[i]] = (int)i;
        UNKNOWN_CAT = idx["unknown"];
        PRODUCTS.init(rows.size());
        for (auto& r : rows) {
            KV* s = PRODUCTS.slot(r.first);
            if (!s->k) s->k = r.first, s->v = (u64)idx[r.second];
        }
    }
    {
        string text = read_small(DATA + "/campaigns.csv");
        CAMP_ID.push_back("organic");
        CAMP_CH.push_back("organic");
        vector<pair<u64, string>> rows;
        for_small_lines(text, [&](const char* p, const char* e) {
            const char* f[2];
            u32 l[2];
            if (!split<2>(p, e, f, l) || !l[0]) return;
            rows.push_back({key_of(f[0], l[0]), string(f[1], l[1])});
        });
        CAMPAIGNS.init(rows.size());
        for (auto& r : rows) {
            KV* s = CAMPAIGNS.slot(r.first);
            if (s->k) continue;
            s->k = r.first;
            s->v = CAMP_ID.size();
            CAMP_ID.push_back(key_str(r.first));
            CAMP_CH.push_back(r.second);
        }
    }
    const int NCAMP = (int)CAMP_ID.size();
    const int NCAT = (int)CATNAMES.size();
    tick("lookups");

    // ---- map input files ---------------------------------------------------
    auto map_all = [&](const string& dir) {
        vector<Mapped> v;
        for (auto& p : list_csv(dir)) v.push_back(map_file(p));
        return v;
    };
    vector<Mapped> evf = map_all(DATA + "/events"), clf = map_all(DATA + "/clicks"),
                   lkf = map_all(DATA + "/links");
    size_t total_bytes = 0;
    for (auto* v : {&evf, &clf, &lkf})
        for (auto& m : *v) total_bytes += m.n;
    size_t chunk = max<size_t>(256 << 10, min<size_t>(8 << 20, total_bytes / (NT * 24) + 1));

    // ---- links ---------------------------------------------------------------
    {
        vector<Task> lt;
        add_tasks(lt, lkf, 2, chunk);
        vector<vector<pair<u64, u64>>> pairs(lt.size());
        par_for((int)lt.size(), [&](int i, int) {
            auto& out = pairs[i];
            for_lines(lt[i], [&](const char* p, const char* e) {
                const char* f[2];
                u32 l[2];
                if (!split<2>(p, e, f, l) || !l[0] || !l[1]) return;
                out.push_back({key_of(f[0], l[0]), key_of(f[1], l[1])});
            });
        });
        // keys by partition
        int NL = (int)pairs.size();
        vector<vector<u64>> lk((size_t)NL * NP);
        par_for(NL, [&](int i, int) {
            for (auto& pr : pairs[i]) {
                lk[(size_t)i * NP + part_of(pr.first)].push_back(pr.first);
                lk[(size_t)i * NP + part_of(pr.second)].push_back(pr.second);
            }
        });
        vector<u32> cnt(NP, 0);
        par_for(NP, [&](int p, int) {
            size_t n = 0;
            for (int i = 0; i < NL; i++) n += lk[(size_t)i * NP + p].size();
            LM[p].init(n);
            u32 c = 0;
            for (int i = 0; i < NL; i++)
                for (u64 k : lk[(size_t)i * NP + p]) {
                    KV* s = LM[p].slot(k);
                    if (!s->k) s->k = k, s->v = c++;
                }
            cnt[p] = c;
        });
        vector<u32> base(NP + 1, 0);
        for (int p = 0; p < NP; p++) base[p + 1] = base[p] + cnt[p];
        u32 N = base[NP];
        vector<u64> keys(N);
        vector<atomic<u32>> par(N);
        par_for(NP, [&](int p, int) {
            for (auto& e : LM[p].t)
                if (e.k) keys[base[p] + e.v] = e.k;
        });
        for (u32 i = 0; i < N; i++) par[i].store(i, memory_order_relaxed);
        auto gidx = [&](u64 k) {
            int p = part_of(k);
            return base[p] + (u32)LM[p].get(k)->v;
        };
        par_for(NL, [&](int i, int) {
            for (auto& pr : pairs[i]) uf_union(par.data(), gidx(pr.first), gidx(pr.second));
        });
        vector<atomic<u64>> nm(N);
        for (u32 i = 0; i < N; i++) nm[i].store(keys[i], memory_order_relaxed);
        int NB = (int)((N + 65535) / 65536);
        par_for(NB, [&](int b, int) {
            u32 lo = (u32)b * 65536, hi = min<u32>(N, lo + 65536);
            for (u32 i = lo; i < hi; i++) {
                u32 r = uf_find(par.data(), i);
                if (r == i) continue;
                u64 k = keys[i];
                u64 cur = nm[r].load(memory_order_relaxed);
                while (key_less(k, cur) && !nm[r].compare_exchange_weak(cur, k)) {
                }
            }
        });
        par_for(NP, [&](int p, int) {
            for (auto& e : LM[p].t)
                if (e.k) e.v = nm[uf_find(par.data(), base[p] + (u32)e.v)].load(memory_order_relaxed);
        });
        tick("links");
    }

    // ---- events + clicks: parse ---------------------------------------------
    vector<vector<EvRec>> EB((size_t)NT * NP);
    vector<vector<CRec>> CB((size_t)NT * NP);
    {
        vector<Task> tasks;
        add_tasks(tasks, evf, 0, chunk);
        add_tasks(tasks, clf, 1, chunk);
        const i64 CWS = WS - WEEK_MS;
        par_for((int)tasks.size(), [&](int i, int tid) {
            const Task& t = tasks[i];
            vector<EvRec>* eb = &EB[(size_t)tid * NP];
            vector<CRec>* cb = &CB[(size_t)tid * NP];
            if (t.kind == 0) {
                for_lines(t, [&](const char* p, const char* e) {
                    const char* f[7];
                    u32 l[7];
                    if (!split<7>(p, e, f, l) || !l[0]) return;
                    EvRec r;
                    r.eid = key_of(f[0], l[0]);
                    i64 ing;
                    if (!parse_ts(f[3], l[3], ing)) ing = INT64_MAX;
                    r.ingest = ing;
                    r.user = 0;
                    r.ts = 0;
                    r.price = 0;
                    r.cat = 0;
                    r.flags = 0;
                    r.pad = 0;
                    do {
                        if (!l[1]) break;
                        i64 ts;
                        if (!parse_ts(f[2], l[2], ts) || ts < WS || ts >= WE) break;
                        int type;
                        if (l[4] == 4 && memcmp(f[4], "view", 4) == 0)
                            type = 0;
                        else if (l[4] == 4 && memcmp(f[4], "cart", 4) == 0)
                            type = 1;
                        else if (l[4] == 8 && memcmp(f[4], "purchase", 8) == 0)
                            type = 2;
                        else
                            break;
                        if (!l[5]) break;
                        u32 price = 0;
                        if (type == 2 && !parse_price(f[6], l[6], price)) break;
                        if (ing - ts > DAY_MS) break;  // late
                        r.user = key_of(f[1], l[1]);
                        r.ts = ts;
                        if (type == 2) {
                            r.price = price;
                            u64 pk = key_find(f[5], l[5]);
                            const KV* pe = pk ? PRODUCTS.get(pk) : nullptr;
                            r.cat = (u16)(pe ? pe->v : (u64)UNKNOWN_CAT);
                            r.flags = 3;
                        } else {
                            r.flags = 1;
                        }
                    } while (0);
                    eb[part_of(r.eid)].push_back(r);
                });
            } else {
                for_lines(t, [&](const char* p, const char* e) {
                    const char* f[4];
                    u32 l[4];
                    if (!split<4>(p, e, f, l) || !l[0] || !l[1] || !l[2]) return;
                    i64 ts;
                    if (!parse_ts(f[3], l[3], ts) || ts < CWS || ts >= WE) return;
                    u64 ck = key_find(f[2], l[2]);
                    const KV* ce = ck ? CAMPAIGNS.get(ck) : nullptr;
                    if (!ce) return;
                    CRec c;
                    c.person = person_of(key_of(f[1], l[1]));
                    c.ts = ts;
                    c.cid = key_of(f[0], l[0]);
                    c.camp = (u32)ce->v;
                    c.pad = 0;
                    cb[part_of(c.person)].push_back(c);
                });
            }
        });
        tick("parse events+clicks");
    }

    // ---- dedup events, map to person -----------------------------------------
    vector<vector<PRec>> PBK((size_t)NT * NP);
    {
        struct DE {
            u64 eid;
            i64 ing;
            const EvRec* r;
        };
        vector<vector<DE>> tabs(NT);
        par_for(NP, [&](int p, int tid) {
            size_t n = 0;
            for (int t = 0; t < NT; t++) n += EB[(size_t)t * NP + p].size();
            if (!n) return;
            size_t cap = 16;
            while (cap < n * 2) cap <<= 1;
            auto& tab = tabs[tid];
            tab.assign(cap, DE{0, 0, nullptr});
            u64 mask = cap - 1;
            for (int t = 0; t < NT; t++)
                for (const EvRec& r : EB[(size_t)t * NP + p]) {
                    u64 i = mix64(r.eid) & mask;
                    while (tab[i].eid && tab[i].eid != r.eid) i = (i + 1) & mask;
                    DE& d = tab[i];
                    if (!d.eid)
                        d = DE{r.eid, r.ingest, &r};
                    else if (r.ingest < d.ing)
                        d.ing = r.ingest, d.r = &r;
                }
            vector<PRec>* pb = &PBK[(size_t)tid * NP];
            for (auto& d : tab) {
                if (!d.eid || !(d.r->flags & 1)) continue;
                const EvRec& r = *d.r;
                PRec q;
                q.person = person_of(r.user);
                q.ts = r.ts;
                q.price = r.price;
                q.cat = r.cat;
                q.purch = (r.flags >> 1) & 1;
                q.pad = 0;
                pb[part_of(q.person)].push_back(q);
            }
        });
        tabs.clear();
        for (auto& v : EB) vector<EvRec>().swap(v);
        tick("dedup");
    }

    // ---- sessions, attribution, aggregation ------------------------------------
    vector<Out> outs(NT);
    for (auto& o : outs) {
        size_t cells = (size_t)NCAMP * NDAYS;
        o.cs.assign(cells, 0);
        o.cps.assign(cells, 0);
        o.crev.assign(cells, 0);
        o.cas.assign(cells, 0);
        o.cnt.assign(NCAMP, 0);
        o.pos.assign(NCAMP, 0);
        o.catsum.assign(NCAT, 0);
    }
    {
        vector<size_t> psize(NP, 0);
        for (int p = 0; p < NP; p++)
            for (int t = 0; t < NT; t++) psize[p] += PBK[(size_t)t * NP + p].size();
        vector<int> order(NP);
        for (int p = 0; p < NP; p++) order[p] = p;
        sort(order.begin(), order.end(), [&](int a, int b) { return psize[a] > psize[b]; });
        par_for(NP, [&](int oi, int tid) {
            int p = order[oi];
            Out& o = outs[tid];
            vector<PRec> ev;
            ev.reserve(psize[p]);
            for (int t = 0; t < NT; t++) {
                auto& v = PBK[(size_t)t * NP + p];
                ev.insert(ev.end(), v.begin(), v.end());
                vector<PRec>().swap(v);
            }
            if (ev.empty()) return;
            sort(ev.begin(), ev.end(), [](const PRec& a, const PRec& b) {
                return a.person != b.person ? a.person < b.person : a.ts < b.ts;
            });
            vector<CRec> cl;
            for (int t = 0; t < NT; t++) {
                auto& v = CB[(size_t)t * NP + p];
                cl.insert(cl.end(), v.begin(), v.end());
            }
            sort(cl.begin(), cl.end(), [](const CRec& a, const CRec& b) {
                if (a.person != b.person) return a.person < b.person;
                if (a.ts != b.ts) return a.ts < b.ts;
                if (a.cid == b.cid) return false;
                return key_less(b.cid, a.cid);  // smallest click_id last
            });
            size_t ne = ev.size(), nc = cl.size(), cp = 0;
            size_t i = 0;
            while (i < ne) {
                u64 person = ev[i].person;
                size_t j = i;
                while (j < ne && ev[j].person == person) j++;
                while (cp < nc && cl[cp].person < person) cp++;
                size_t ca = cp;
                while (cp < nc && cl[cp].person == person) cp++;
                size_t cb = cp, lo = ca, hi = ca;
                // sessions
                i64 cur_day = INT64_MIN;
                i64 d_ses = 0, d_evs = 0, d_rev = 0;
                auto flush_day = [&]() {
                    if (cur_day == INT64_MIN) return;
                    int best = -1;
                    i64 bestv = -1;
                    for (int c : o.touched) {
                        i64 v = o.catsum[c];
                        if (v > bestv || (v == bestv && c < best)) bestv = v, best = c;
                        o.catsum[c] = 0;
                    }
                    o.touched.clear();
                    o.uid.push_back(person);
                    o.day.push_back((i32)cur_day);
                    o.ses.push_back(d_ses);
                    o.evs.push_back(d_evs);
                    o.rev.push_back(d_rev);
                    o.top.push_back(best);
                    d_ses = d_evs = d_rev = 0;
                };
                size_t s = i;
                while (s < j) {
                    const i64 start = ev[s].ts;
                    size_t e = s + 1;
                    while (e < j && ev[e].ts - ev[e - 1].ts <= GAP_MS) e++;
                    const i64 day = floordiv(start, DAY_MS);
                    if (day != cur_day) {
                        flush_day();
                        cur_day = day;
                    }
                    i64 rev = 0;
                    bool purch = false;
                    for (size_t k = s; k < e; k++)
                        if (ev[k].purch) {
                            purch = true;
                            rev += ev[k].price;
                            u16 c = ev[k].cat;
                            if (o.catsum[c] == 0) o.touched.push_back(c);
                            o.catsum[c] += ev[k].price;
                        }
                    d_ses += 1;
                    d_evs += (i64)(e - s);
                    d_rev += rev;
                    // attribution: clicks with start - 7d <= ts <= start
                    while (hi < cb && cl[hi].ts <= start) {
                        u32 c = cl[hi].camp;
                        if (o.cnt[c]++ == 0) o.pos[c] = (int)o.active.size(), o.active.push_back((int)c);
                        hi++;
                    }
                    while (lo < hi && cl[lo].ts < start - WEEK_MS) {
                        u32 c = cl[lo].camp;
                        if (--o.cnt[c] == 0) {
                            int at = o.pos[c], last = o.active.back();
                            o.active[at] = last;
                            o.pos[last] = at;
                            o.active.pop_back();
                        }
                        lo++;
                    }
                    u32 credited = hi > lo ? cl[hi - 1].camp : 0;
                    size_t di = (size_t)(day - DAY0);
                    size_t cell = (size_t)credited * NDAYS + di;
                    o.cs[cell] += 1;
                    o.cps[cell] += purch;
                    o.crev[cell] += rev;
                    for (int c : o.active) o.cas[(size_t)c * NDAYS + di] += 1;
                    s = e;
                }
                flush_day();
                for (int c : o.active) o.cnt[c] = 0;
                o.active.clear();
                i = j;
            }
        });
        for (auto& v : CB) vector<CRec>().swap(v);
        tick("sessions");
    }

    // ---- write outputs ------------------------------------------------------------
    string UD = OUT + "/user_daily", CD = OUT + "/campaign_daily";
    mkdir(OUT.c_str(), 0777);
    mkdir(UD.c_str(), 0777);
    mkdir(CD.c_str(), 0777);
    {
        size_t total_rows = 0;
        for (auto& o : outs) total_rows += o.uid.size();
        par_for(NT, [&](int t, int) {
            Out& o = outs[t];
            size_t n = o.uid.size();
            if (n == 0 && !(t == 0 && total_rows == 0)) return;
            vector<string> ids(n);
            for (size_t r = 0; r < n; r++) ids[r] = key_str(o.uid[r]);
            vector<const string*> top(n);
            for (size_t r = 0; r < n; r++) top[r] = o.top[r] < 0 ? nullptr : &CATNAMES[o.top[r]];
            vector<ColData> cols(6);
            cols[0] = {"user_id", PT_BYTES, false, 0, 1, ""};
            page_strings(cols[0].page, ids);
            cols[1] = {"day", PT_INT32, false, 6, 6, ""};
            cols[1].page.append((const char*)o.day.data(), n * 4);
            cols[2] = {"sessions", PT_INT64, false, -1, 0, ""};
            cols[2].page.append((const char*)o.ses.data(), n * 8);
            cols[3] = {"events", PT_INT64, false, -1, 0, ""};
            cols[3].page.append((const char*)o.evs.data(), n * 8);
            cols[4] = {"revenue_cents", PT_INT64, false, -1, 0, ""};
            cols[4].page.append((const char*)o.rev.data(), n * 8);
            cols[5] = {"top_category", PT_BYTES, true, 0, 1, ""};
            page_opt_strings(cols[5].page, top);
            write_parquet(UD + "/part-" + to_string(t) + ".parquet", (i64)n, cols);
        });
    }
    {
        size_t cells = (size_t)NCAMP * NDAYS;
        vector<i64> cs(cells, 0), cps(cells, 0), crev(cells, 0), cas(cells, 0);
        for (auto& o : outs)
            for (size_t x = 0; x < cells; x++) {
                cs[x] += o.cs[x];
                cps[x] += o.cps[x];
                crev[x] += o.crev[x];
                cas[x] += o.cas[x];
            }
        vector<string> id, ch;
        vector<i32> day;
        vector<i64> a, b, c, d;
        for (int k = 0; k < NCAMP; k++)
            for (int di = 0; di < NDAYS; di++) {
                size_t x = (size_t)k * NDAYS + di;
                if (!cs[x] && !cas[x]) continue;
                id.push_back(CAMP_ID[k]);
                ch.push_back(CAMP_CH[k]);
                day.push_back((i32)(DAY0 + di));
                a.push_back(cs[x]);
                b.push_back(cps[x]);
                c.push_back(crev[x]);
                d.push_back(cas[x]);
            }
        size_t n = id.size();
        vector<ColData> cols(7);
        cols[0] = {"campaign_id", PT_BYTES, false, 0, 1, ""};
        page_strings(cols[0].page, id);
        cols[1] = {"channel", PT_BYTES, false, 0, 1, ""};
        page_strings(cols[1].page, ch);
        cols[2] = {"day", PT_INT32, false, 6, 6, ""};
        cols[2].page.append((const char*)day.data(), n * 4);
        cols[3] = {"sessions", PT_INT64, false, -1, 0, ""};
        cols[3].page.append((const char*)a.data(), n * 8);
        cols[4] = {"purchasing_sessions", PT_INT64, false, -1, 0, ""};
        cols[4].page.append((const char*)b.data(), n * 8);
        cols[5] = {"revenue_cents", PT_INT64, false, -1, 0, ""};
        cols[5].page.append((const char*)c.data(), n * 8);
        cols[6] = {"assisted_sessions", PT_INT64, false, -1, 0, ""};
        cols[6].page.append((const char*)d.data(), n * 8);
        write_parquet(CD + "/part-0.parquet", (i64)n, cols);
    }
    tick("write");
    fflush(stdout);
    _exit(0);  // skip freeing large structures
}
