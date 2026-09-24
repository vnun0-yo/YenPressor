/* YenPressor.c — modern archive extractor
 * Build: gcc -O2 -std=c11 -Wall -Wextra -pthread -o YenPressor YenPressor.c -larchive -lz -lcrypto
 */

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <ctype.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <getopt.h>
#include <limits.h>
#include <termios.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <pthread.h>
#include <archive.h>
#include <archive_entry.h>

/* ─────────────────────────────────────────────────────────────
 * Constants
 * ───────────────────────────────────────────────────────────── */

#define YEN_VERSION          "3.2.0"
#define YEN_MAX_PATH         4096
#define YEN_BUFFER_SIZE      262144
#define YEN_DEFAULT_DEPTH    10
#define YEN_MAX_DEPTH        64
#define YEN_PROGRESS_MIN     1
#define YEN_MAX_PASSWORD     1024

/* ─────────────────────────────────────────────────────────────
 * ANSI colors
 * ───────────────────────────────────────────────────────────── */

#define C_RESET     "\033[0m"
#define C_BOLD      "\033[1m"
#define C_RED       "\033[1;31m"
#define C_GREEN     "\033[1;32m"
#define C_YELLOW    "\033[1;33m"
#define C_MAGENTA   "\033[1;35m"
#define C_CYAN      "\033[1;36m"
#define C_WHITE     "\033[1;37m"
#define C_GRAY      "\033[0;90m"

/* ─────────────────────────────────────────────────────────────
 * Types
 * ───────────────────────────────────────────────────────────── */

typedef enum {
    YEN_OK = 0,
    YEN_ERR_ARGS,
    YEN_ERR_OPEN,
    YEN_ERR_FORMAT,
    YEN_ERR_PASSWORD,
    YEN_ERR_WRITE,
    YEN_ERR_MEMORY,
    YEN_ERR_DISK,
    YEN_ERR_UNSAFE,
    YEN_ERR_LIMIT,
    YEN_ERR_INTERNAL
} yen_status_t;

typedef enum {
    YEN_TYPE_UNKNOWN = 0,
    YEN_TYPE_ZIP,
    YEN_TYPE_RAR,
    YEN_TYPE_RAR5,
    YEN_TYPE_7Z,
    YEN_TYPE_TAR,
    YEN_TYPE_GZ,
    YEN_TYPE_BZ2,
    YEN_TYPE_XZ,
    YEN_TYPE_ZSTD,
    YEN_TYPE_LZ4,
    YEN_TYPE_LZMA,
    YEN_TYPE_CAB,
    YEN_TYPE_ISO,
    YEN_TYPE_CPIO,
    YEN_TYPE_AR,
    YEN_TYPE_DEB,
    YEN_TYPE_RPM,
    YEN_TYPE_TAR_GZ,
    YEN_TYPE_TAR_BZ2,
    YEN_TYPE_TAR_XZ,
    YEN_TYPE_TAR_ZSTD
} yen_type_t;

typedef enum {
    LOG_ERROR = 0,
    LOG_WARN,
    LOG_INFO,
    LOG_DEBUG,
    LOG_TRACE
} log_level_t;

typedef struct {
    const char *output_dir;
    const char *specific_file;
    const char *password;
    int list_only;
    int verbose;
    int quiet;
    int recursive;
    int max_depth;
    uint64_t max_extract;
    int preserve_perms;
    int overwrite;
    int no_color;
    int no_progress;
} yen_options_t;

typedef struct {
    uint64_t archives_processed;
    uint64_t files_extracted;
    uint64_t dirs_created;
    uint64_t total_bytes;
    uint64_t files_skipped;
    uint64_t unsafe_paths;
    uint64_t errors;
    uint64_t total_files;
    uint64_t start_ms;
    char current_file[YEN_MAX_PATH];
    pthread_mutex_t lock;
} yen_stats_t;

typedef struct {
    char path[YEN_MAX_PATH];
    yen_type_t type;
    int depth;
} nested_task_t;

typedef struct {
    nested_task_t *items;
    size_t count;
    size_t cap;
    pthread_mutex_t lock;
} task_queue_t;

/* ─────────────────────────────────────────────────────────────
 * Globals
 * ───────────────────────────────────────────────────────────── */

static yen_stats_t g_stats = {0};
static log_level_t g_log_level = LOG_INFO;
static int g_log_quiet = 0;
static int g_use_color = 1;
static int g_progress_active = 0;
static int g_cpu_cores = 4;
static task_queue_t g_queue = {0};

/* ─────────────────────────────────────────────────────────────
 * Forward declarations
 * ───────────────────────────────────────────────────────────── */

static void clear_progress(void);
static void render_progress(void);

/* ─────────────────────────────────────────────────────────────
 * Color helpers
 * ───────────────────────────────────────────────────────────── */

static const char *col(const char *code) {
    return g_use_color ? code : "";
}

static const char *rst(void) {
    return g_use_color ? C_RESET : "";
}

/* ─────────────────────────────────────────────────────────────
 * Logger
 * ───────────────────────────────────────────────────────────── */

static const char *level_icon(log_level_t l) {
    switch (l) {
        case LOG_ERROR: return "✗";
        case LOG_WARN:  return "⚠";
        case LOG_INFO:  return "✓";
        case LOG_DEBUG: return "·";
        case LOG_TRACE: return "·";
    }
    return "·";
}

static const char *level_color_code(log_level_t l) {
    switch (l) {
        case LOG_ERROR: return C_RED;
        case LOG_WARN:  return C_YELLOW;
        case LOG_INFO:  return C_GREEN;
        case LOG_DEBUG: return C_CYAN;
        case LOG_TRACE: return C_GRAY;
    }
    return "";
}

static void log_msg(log_level_t level, const char *fmt, ...) {
    if (level > g_log_level) return;
    if (g_log_quiet && level > LOG_WARN) return;

    pthread_mutex_lock(&g_stats.lock);

    if (g_progress_active) {
        fprintf(stderr, "\r\033[K\033[1A\r\033[K");
        g_progress_active = 0;
    }

    FILE *out = (level <= LOG_WARN) ? stderr : stdout;

    fprintf(out, "%s%s%s %s", col(level_color_code(level)),
            level_icon(level), rst(), col(C_GRAY));

    va_list ap;
    va_start(ap, fmt);
    vfprintf(out, fmt, ap);
    va_end(ap);

    fprintf(out, "%s\n", rst());
    fflush(out);

    pthread_mutex_unlock(&g_stats.lock);
}

#define LOG_E(...) log_msg(LOG_ERROR, __VA_ARGS__)
#define LOG_W(...) log_msg(LOG_WARN, __VA_ARGS__)
#define LOG_I(...) log_msg(LOG_INFO, __VA_ARGS__)
#define LOG_D(...) log_msg(LOG_DEBUG, __VA_ARGS__)
#define LOG_T(...) log_msg(LOG_TRACE, __VA_ARGS__)

static void log_raw(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stdout, fmt, ap);
    va_end(ap);
    fflush(stdout);
}

/* ─────────────────────────────────────────────────────────────
 * Utils
 * ───────────────────────────────────────────────────────────── */

static uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

static char *xstrdup(const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s) + 1;
    char *p = malloc(n);
    if (p) memcpy(p, s, n);
    return p;
}

static const char *base_name(const char *path) {
    if (!path) return "";
    const char *p = strrchr(path, '/');
    return p ? p + 1 : path;
}

static int file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int is_directory(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return S_ISDIR(st.st_mode);
}

static int is_regular_file(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return S_ISREG(st.st_mode);
}

static uint64_t file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return (uint64_t)st.st_size;
}

static uint64_t disk_free(const char *path) {
    struct statvfs st;
    if (statvfs(path, &st) != 0) return 0;
    return (uint64_t)st.f_bavail * (uint64_t)st.f_frsize;
}

static int mkdir_p(const char *path) {
    if (!path || !*path) return -1;
    char tmp[YEN_MAX_PATH];
    size_t len = strlen(path);
    if (len >= sizeof(tmp)) return -1;
    memcpy(tmp, path, len + 1);
    if (tmp[len - 1] == '/') tmp[len - 1] = '\0';
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
    return 0;
}

static void human_size(uint64_t bytes, char *buf, size_t buf_size) {
    const char *units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    double v = (double)bytes;
    int u = 0;
    while (v >= 1024.0 && u < 5) { v /= 1024.0; u++; }
    snprintf(buf, buf_size, "%.2f %s", v, units[u]);
}

static void human_rate(double bytes_per_sec, char *buf, size_t buf_size) {
    const char *units[] = {"B", "KB", "MB", "GB"};
    double v = bytes_per_sec;
    int u = 0;
    while (v >= 1024.0 && u < 3) { v /= 1024.0; u++; }
    snprintf(buf, buf_size, "%.1f %s/s", v, units[u]);
}

static uint64_t parse_size(const char *s) {
    if (!s) return 0;
    char *end = NULL;
    double v = strtod(s, &end);
    if (end == s) return 0;
    while (*end && isspace((unsigned char)*end)) end++;
    uint64_t mult = 1;
    if (*end) {
        char c = (char)toupper((unsigned char)*end);
        switch (c) {
            case 'K': mult = 1024ULL; break;
            case 'M': mult = 1024ULL * 1024; break;
            case 'G': mult = 1024ULL * 1024 * 1024; break;
            case 'T': mult = 1024ULL * 1024 * 1024 * 1024; break;
            case 'B': mult = 1; break;
            default: return 0;
        }
    }
    return (uint64_t)(v * (double)mult);
}

static void format_eta(double seconds, char *buf, size_t buf_size) {
    if (seconds < 0 || seconds > 86400 * 7) {
        snprintf(buf, buf_size, "--");
    } else if (seconds < 60) {
        snprintf(buf, buf_size, "%.0fs", seconds);
    } else if (seconds < 3600) {
        snprintf(buf, buf_size, "%dm%02ds", (int)(seconds / 60), (int)seconds % 60);
    } else {
        snprintf(buf, buf_size, "%dh%02dm",
                 (int)(seconds / 3600), (int)((int)seconds % 3600) / 60);
    }
}

static void format_current_file(const char *path, char *buf, size_t buf_size) {
    if (!path || !*path) {
        snprintf(buf, buf_size, "...");
        return;
    }
    size_t len = strlen(path);
    if (len < buf_size) {
        memcpy(buf, path, len + 1);
        return;
    }
    size_t head = (buf_size - 4) / 2;
    size_t tail = buf_size - 4 - head;
    memcpy(buf, path, head);
    buf[head] = '.';
    buf[head + 1] = '.';
    buf[head + 2] = '.';
    memcpy(buf + head + 3, path + len - tail, tail);
    buf[buf_size - 1] = '\0';
}

static int terminal_width(void) {
    struct winsize ws;
    if (ioctl(STDERR_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0) {
        return ws.ws_col;
    }
    return 80;
}

static int detect_cpu_cores(void) {
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) n = 4;
    if (n > 32) n = 32;
    return (int)n;
}

/* ─────────────────────────────────────────────────────────────
 * Safe paths
 * ───────────────────────────────────────────────────────────── */

static int is_safe_path(const char *path) {
    if (!path || !*path) return 0;
    if (path[0] == '/') return 0;
    if (path[0] == '\\') return 0;
    if (strstr(path, "../")) return 0;
    if (strstr(path, "..\\")) return 0;
    if (strcmp(path, "..") == 0) return 0;
    if (strstr(path, "/..")) return 0;
    size_t len = strlen(path);
    if (len >= YEN_MAX_PATH) return 0;
    return 1;
}

static int normalize_path(const char *path, char *out, size_t out_size) {
    if (!path || !out || out_size == 0) return -1;
    size_t w = 0;
    const char *p = path;
    while (*p && w + 1 < out_size) {
        if (p[0] == '.' && p[1] == '/') { p += 2; continue; }
        if (p[0] == '.' && p[1] == '.' && p[2] == '/') {
            if (w > 0) {
                while (w > 0 && out[w - 1] != '/') w--;
                if (w > 0 && out[w - 1] == '/') w--;
            }
            p += 3;
            continue;
        }
        out[w++] = *p++;
    }
    out[w] = '\0';
    return 0;
}

static int path_inside(const char *base, const char *target) {
    if (!base || !target) return 0;
    size_t lb = strlen(base);
    if (strncmp(base, target, lb) != 0) return 0;
    if (target[lb] != '\0' && target[lb] != '/') return 0;
    return 1;
}

/* ─────────────────────────────────────────────────────────────
 * External tools
 * ───────────────────────────────────────────────────────────── */

static int which_binary(const char *name, char *out, size_t out_size) {
    const char *path_env = getenv("PATH");
    if (!path_env) return 0;
    char *paths = xstrdup(path_env);
    if (!paths) return 0;

    char *saveptr = NULL;
    char *dir = strtok_r(paths, ":", &saveptr);
    int found = 0;

    while (dir) {
        char candidate[YEN_MAX_PATH];
        int n = snprintf(candidate, sizeof(candidate), "%s/%s", dir, name);
        if (n > 0 && (size_t)n < sizeof(candidate) &&
            access(candidate, X_OK) == 0) {
            snprintf(out, out_size, "%s", candidate);
            found = 1;
            break;
        }
        dir = strtok_r(NULL, ":", &saveptr);
    }

    free(paths);
    return found;
}

/* ─────────────────────────────────────────────────────────────
 * Type detection
 * ───────────────────────────────────────────────────────────── */

typedef struct {
    const char *magic;
    size_t magic_len;
    size_t offset;
    yen_type_t type;
} magic_entry_t;

static const magic_entry_t g_magics[] = {
    { "PK\x03\x04", 4, 0, YEN_TYPE_ZIP },
    { "PK\x05\x06", 4, 0, YEN_TYPE_ZIP },
    { "PK\x07\x08", 4, 0, YEN_TYPE_ZIP },
    { "Rar!\x1a\x07\x01\x00", 8, 0, YEN_TYPE_RAR5 },
    { "Rar!\x1a\x07\x00", 7, 0, YEN_TYPE_RAR },
    { "7z\xbc\xaf\x27\x1c", 6, 0, YEN_TYPE_7Z },
    { "\x1f\x8b", 2, 0, YEN_TYPE_GZ },
    { "BZh", 3, 0, YEN_TYPE_BZ2 },
    { "\xfd\x37\x7a\x58\x5a\x00", 6, 0, YEN_TYPE_XZ },
    { "\x28\xb5\x2f\xfd", 4, 0, YEN_TYPE_ZSTD },
    { "\x04\x22\x4d\x18", 4, 0, YEN_TYPE_LZ4 },
    { "\x5d\x00\x00", 3, 0, YEN_TYPE_LZMA },
    { "MSCF", 4, 0, YEN_TYPE_CAB },
    { "070701", 6, 0, YEN_TYPE_CPIO },
    { "070707", 6, 0, YEN_TYPE_CPIO },
    { "!<arch>\n", 8, 0, YEN_TYPE_AR },
    { "!<arch>\ndebian", 14, 0, YEN_TYPE_DEB },
    { "\xed\xab\xee\xdb", 4, 0, YEN_TYPE_RPM },
};

static const char *g_type_names[] = {
    "unknown", "ZIP", "RAR", "RAR5", "7-Zip", "TAR", "gzip",
    "bzip2", "xz", "zstd", "lz4", "lzma", "CAB", "ISO",
    "cpio", "ar", "deb", "rpm", "tar.gz", "tar.bz2",
    "tar.xz", "tar.zstd"
};

static const char *type_name(yen_type_t t) {
    if (t < 0 || t > YEN_TYPE_TAR_ZSTD) return "unknown";
    return g_type_names[t];
}

static const char *type_color(yen_type_t t) {
    if (!g_use_color) return "";
    switch (t) {
        case YEN_TYPE_ZIP:      return C_YELLOW;
        case YEN_TYPE_RAR:
        case YEN_TYPE_RAR5:     return C_MAGENTA;
        case YEN_TYPE_7Z:       return C_CYAN;
        case YEN_TYPE_TAR:
        case YEN_TYPE_TAR_GZ:
        case YEN_TYPE_TAR_BZ2:
        case YEN_TYPE_TAR_XZ:
        case YEN_TYPE_TAR_ZSTD: return C_GREEN;
        default:                return C_GRAY;
    }
}

static yen_type_t detect_type(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return YEN_TYPE_UNKNOWN;
    unsigned char buf[8192];
    size_t n = fread(buf, 1, sizeof(buf), f);
    fclose(f);
    if (n == 0) return YEN_TYPE_UNKNOWN;

    for (size_t i = 0; i < sizeof(g_magics) / sizeof(g_magics[0]); i++) {
        const magic_entry_t *m = &g_magics[i];
        if (m->offset + m->magic_len > n) continue;
        if (memcmp(buf + m->offset, m->magic, m->magic_len) == 0) {
            return m->type;
        }
    }
    if (n >= 265 && memcmp(buf + 257, "ustar", 5) == 0) {
        return YEN_TYPE_TAR;
    }
    return YEN_TYPE_UNKNOWN;
}

static int is_archive_type(yen_type_t t) {
    switch (t) {
        case YEN_TYPE_ZIP:
        case YEN_TYPE_RAR:
        case YEN_TYPE_RAR5:
        case YEN_TYPE_7Z:
        case YEN_TYPE_TAR:
        case YEN_TYPE_CAB:
        case YEN_TYPE_ISO:
        case YEN_TYPE_CPIO:
        case YEN_TYPE_AR:
        case YEN_TYPE_DEB:
        case YEN_TYPE_RPM:
        case YEN_TYPE_TAR_GZ:
        case YEN_TYPE_TAR_BZ2:
        case YEN_TYPE_TAR_XZ:
        case YEN_TYPE_TAR_ZSTD:
            return 1;
        default:
            return 0;
    }
}

static int is_compressed_only(yen_type_t t) {
    switch (t) {
        case YEN_TYPE_GZ:
        case YEN_TYPE_BZ2:
        case YEN_TYPE_XZ:
        case YEN_TYPE_ZSTD:
        case YEN_TYPE_LZ4:
        case YEN_TYPE_LZMA:
            return 1;
        default:
            return 0;
    }
}

/* ─────────────────────────────────────────────────────────────
 * Task queue
 * ───────────────────────────────────────────────────────────── */

static void queue_init(task_queue_t *q) {
    q->items = NULL;
    q->count = 0;
    q->cap = 0;
    pthread_mutex_init(&q->lock, NULL);
}

static void queue_free(task_queue_t *q) {
    free(q->items);
    pthread_mutex_destroy(&q->lock);
}

static void queue_push(task_queue_t *q, const char *path, yen_type_t t, int depth) {
    pthread_mutex_lock(&q->lock);
    if (q->count >= q->cap) {
        size_t new_cap = q->cap ? q->cap * 2 : 16;
        nested_task_t *new_items = realloc(q->items, new_cap * sizeof(*new_items));
        if (!new_items) {
            pthread_mutex_unlock(&q->lock);
            return;
        }
        q->items = new_items;
        q->cap = new_cap;
    }
    strncpy(q->items[q->count].path, path, YEN_MAX_PATH - 1);
    q->items[q->count].path[YEN_MAX_PATH - 1] = '\0';
    q->items[q->count].type = t;
    q->items[q->count].depth = depth;
    q->count++;
    pthread_mutex_unlock(&q->lock);
}

/* ─────────────────────────────────────────────────────────────
 * Progress bar
 * ───────────────────────────────────────────────────────────── */

static void render_progress(void) {
    static uint64_t last_render_ms = 0;
    uint64_t now = now_ms();
    if (now - last_render_ms < 80) return;
    last_render_ms = now;
    if (g_log_quiet || g_stats.total_files < YEN_PROGRESS_MIN) return;
    if (!isatty(STDERR_FILENO)) return;

    uint64_t elapsed = now_ms() - g_stats.start_ms;
    if (elapsed == 0) elapsed = 1;

    double ratio = (double)g_stats.files_extracted / (double)g_stats.total_files;
    if (ratio > 1.0) ratio = 1.0;

    int width = terminal_width();
    int bar_width = width - 50;
    if (bar_width < 15) bar_width = 15;
    if (bar_width > 45) bar_width = 45;

    int filled = (int)(ratio * bar_width);

    double speed = (double)g_stats.total_bytes / (elapsed / 1000.0);
    char rate[32];
    human_rate(speed, rate, sizeof(rate));

    double eta_sec = -1.0;
    if (ratio > 0.001) {
        eta_sec = (elapsed / 1000.0) * (1.0 - ratio) / ratio;
    }
    char eta[32];
    format_eta(eta_sec, eta, sizeof(eta));

    char current[60];
    format_current_file(g_stats.current_file, current, sizeof(current));

    char bar[64];
    int pos = 0;
    for (int i = 0; i < bar_width && pos < (int)sizeof(bar) - 1; i++) {
        bar[pos++] = (i < filled) ? '#' : '.';
    }
    bar[pos] = '\0';

    if (g_use_color) {
        fprintf(stderr,
            "\r%s[%s%s%s%s]%s %s%3d%%%s %s(%lu/%lu)%s %s·%s %s%s%s %s·%s %sETA %s%s\033[K",
            col(C_CYAN),
            col(C_GREEN), bar, rst(), col(C_CYAN), rst(),
            col(C_WHITE), (int)(ratio * 100), rst(),
            col(C_GRAY),
            (unsigned long)g_stats.files_extracted,
            (unsigned long)g_stats.total_files,
            rst(),
            col(C_YELLOW), rst(),
            col(C_WHITE), rate, rst(),
            col(C_YELLOW), rst(),
            col(C_GRAY), eta, rst());

        fprintf(stderr, "\n%s  %s%s\033[K\033[1A",
                col(C_GRAY), current, rst());
    } else {
        fprintf(stderr,
            "\r[%s] %3d%% (%lu/%lu) · %s · ETA %s\033[K\n"
            "  %s\033[K\033[1A",
            bar, (int)(ratio * 100),
            (unsigned long)g_stats.files_extracted,
            (unsigned long)g_stats.total_files,
            rate, eta, current);
    }
    fflush(stderr);
    g_progress_active = 1;
}

static void clear_progress(void) {
    if (!g_progress_active) return;
    fprintf(stderr, "\r\033[K\033[1A\r\033[K");
    fflush(stderr);
    g_progress_active = 0;
}

static uint64_t count_files_in_archive(const char *path) {
    struct archive *a = archive_read_new();
    struct archive_entry *entry;
    uint64_t count = 0;

    archive_read_support_filter_all(a);
    archive_read_support_format_all(a);

    if (archive_read_open_filename(a, path, YEN_BUFFER_SIZE) != ARCHIVE_OK) {
        archive_read_free(a);
        return 0;
    }

    while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {
        if (archive_entry_filetype(entry) != AE_IFDIR) {
            count++;
        }
        archive_read_data_skip(a);
    }

    archive_read_close(a);
    archive_read_free(a);
    return count;
}

/* ─────────────────────────────────────────────────────────────
 * Password prompt
 * ───────────────────────────────────────────────────────────── */

static const char *prompt_password(const char *archive_name, char *buffer, size_t buf_size) {
    clear_progress();

    fprintf(stdout, "\n%s⚠  %s%s%s needs a password%s\n",
            col(C_YELLOW), col(C_WHITE), archive_name, col(C_YELLOW), rst());
    fprintf(stdout, "%s   Enter password:%s ", col(C_WHITE), rst());
    fflush(stdout);

    struct termios oldt, newt;
    int have_tty = isatty(STDIN_FILENO);
    int echo_off = 0;

    if (have_tty && tcgetattr(STDIN_FILENO, &oldt) == 0) {
        newt = oldt;
        newt.c_lflag &= ~(tcflag_t)ECHO;
        if (tcsetattr(STDIN_FILENO, TCSANOW, &newt) == 0) {
            echo_off = 1;
        }
    }

    char *result = fgets(buffer, (int)buf_size, stdin);

    if (echo_off) {
        tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
        fprintf(stdout, "\n");
    }

    if (!result) return NULL;

    size_t len = strlen(buffer);
    while (len > 0 && (buffer[len-1] == '\n' || buffer[len-1] == '\r')) {
        buffer[--len] = '\0';
    }

    if (len == 0) return NULL;
    return buffer;
}

/* ─────────────────────────────────────────────────────────────
 * List
 * ───────────────────────────────────────────────────────────── */

static yen_status_t list_archive(const char *path) {
    struct archive *a = archive_read_new();
    struct archive_entry *entry;

    archive_read_support_filter_all(a);
    archive_read_support_format_all(a);

    if (archive_read_open_filename(a, path, YEN_BUFFER_SIZE) != ARCHIVE_OK) {
        LOG_E("cannot open: %s", archive_error_string(a));
        archive_read_free(a);
        return YEN_ERR_OPEN;
    }

    log_raw("%s%-14s %-20s %s%s\n", col(C_CYAN),
            "Size", "Modified", "Name", rst());
    log_raw("%s%s%s\n", col(C_GRAY),
            "────────────────────────────────────────────────────────────────",
            rst());

    int count = 0;
    uint64_t total = 0;

    while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {
        const char *name = archive_entry_pathname(entry);
        la_int64_t size = archive_entry_size(entry);
        time_t mtime = archive_entry_mtime(entry);
        char timebuf[32] = "-";
        if (mtime > 0) {
            struct tm *tm = localtime(&mtime);
            if (tm) strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M", tm);
        }
        int is_dir = (archive_entry_filetype(entry) == AE_IFDIR);
        char human[32];
        human_size((uint64_t)size, human, sizeof(human));

        log_raw("%s%-14s%s %s%-20s%s %s%s%s%s%s\n",
                col(C_GRAY), human, rst(),
                col(C_GRAY), timebuf, rst(),
                is_dir ? col(C_CYAN) : col(C_WHITE), name, rst(),
                is_dir ? "/" : "",
                is_dir ? col(C_GRAY) : "");

        if (!is_dir) total += (uint64_t)size;
        count++;
        archive_read_data_skip(a);
    }

    char human[32];
    human_size(total, human, sizeof(human));

    log_raw("%s%s%s\n", col(C_GRAY),
            "────────────────────────────────────────────────────────────────",
            rst());
    log_raw("%sTotal:%s %d entries, %s%s%s uncompressed\n",
            col(C_WHITE), rst(),
            count, col(C_YELLOW), human, rst());

    archive_read_close(a);
    archive_read_free(a);
    return YEN_OK;
}

/* ─────────────────────────────────────────────────────────────
 * Fast external extractors (max speed)
 * ───────────────────────────────────────────────────────────── */

static yen_status_t fast_extract_7z(const char *archive,
                                    const char *output_dir,
                                    const char *specific_file,
                                    const char *password,
                                    const yen_options_t *opts) {
    (void)opts;
    char tool[YEN_MAX_PATH];
    const char *candidates[] = {"7zz", "7z", "7za", NULL};
    int found = 0;
    for (int i = 0; candidates[i]; i++) {
        if (which_binary(candidates[i], tool, sizeof(tool))) {
            found = 1;
            break;
        }
    }
    if (!found) return YEN_ERR_FORMAT;

    pid_t pid = fork();
    if (pid < 0) return YEN_ERR_INTERNAL;

    if (pid == 0) {
        char opt_o[YEN_MAX_PATH + 4];
        snprintf(opt_o, sizeof(opt_o), "-o%s", output_dir);

        char opt_mt[32];
        snprintf(opt_mt, sizeof(opt_mt), "-mmt=%d", g_cpu_cores);

        char opt_p[YEN_MAX_PASSWORD + 4];
        if (password && *password) {
            snprintf(opt_p, sizeof(opt_p), "-p%s", password);
        } else {
            snprintf(opt_p, sizeof(opt_p), "-p");
        }

        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            dup2(devnull, STDERR_FILENO);
            close(devnull);
        }

        if (specific_file) {
            execl(tool, tool, "e", "-y", "-bd",
                  opt_mt, opt_p, archive, specific_file, opt_o, (char *)NULL);
        } else {
            execl(tool, tool, "x", "-y", "-bd",
                  opt_mt, opt_p, archive, opt_o, (char *)NULL);
        }
        _exit(127);
    }

    int status = 0;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        if (code == 0) return YEN_OK;
        if (code == 2 || code == 8) return YEN_ERR_PASSWORD;
    }
    return YEN_ERR_FORMAT;
}

static yen_status_t fast_extract_zip(const char *archive,
                                     const char *output_dir,
                                     const char *specific_file,
                                     const char *password,
                                     const yen_options_t *opts) {
    (void)opts;
    char tool[YEN_MAX_PATH];
    if (!which_binary("unzip", tool, sizeof(tool))) return YEN_ERR_FORMAT;

    pid_t pid = fork();
    if (pid < 0) return YEN_ERR_INTERNAL;

    if (pid == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            dup2(devnull, STDERR_FILENO);
            close(devnull);
        }
        if (password && *password) {
            if (specific_file) {
                execl(tool, tool, "-o", "-qq", "-P", password, archive,
                      specific_file, "-d", output_dir, (char *)NULL);
            } else {
                execl(tool, tool, "-o", "-qq", "-P", password, archive,
                      "-d", output_dir, (char *)NULL);
            }
        } else {
            if (specific_file) {
                execl(tool, tool, "-o", "-qq", archive,
                      specific_file, "-d", output_dir, (char *)NULL);
            } else {
                execl(tool, tool, "-o", "-qq", archive,
                      "-d", output_dir, (char *)NULL);
            }
        }
        _exit(127);
    }

    int status = 0;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        if (code == 0) return YEN_OK;
        if (code == 82) return YEN_ERR_PASSWORD;
    }
    return YEN_ERR_FORMAT;
}

static yen_status_t fast_extract_rar(const char *archive,
                                     const char *output_dir,
                                     const char *specific_file,
                                     const char *password,
                                     const yen_options_t *opts) {
    (void)opts;
    char tool[YEN_MAX_PATH];
    const char *candidates[] = {"unrar", "unrar-free", NULL};
    int found = 0;
    for (int i = 0; candidates[i]; i++) {
        if (which_binary(candidates[i], tool, sizeof(tool))) {
            found = 1;
            break;
        }
    }
    if (!found) return YEN_ERR_FORMAT;

    pid_t pid = fork();
    if (pid < 0) return YEN_ERR_INTERNAL;

    if (pid == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            dup2(devnull, STDERR_FILENO);
            close(devnull);
        }

        char opt_o[YEN_MAX_PATH + 2];
        snprintf(opt_o, sizeof(opt_o), "%s/", output_dir);

        char opt_p[YEN_MAX_PASSWORD + 4];
        int has_pass = (password && *password);
        if (has_pass) {
            snprintf(opt_p, sizeof(opt_p), "-p%s", password);
        } else {
            opt_p[0] = '\0';
        }

        if (strstr(tool, "unrar-free") == NULL) {
            char opt_ht[16];
            snprintf(opt_ht, sizeof(opt_ht), "-ht%d", g_cpu_cores);
            if (has_pass) {
                if (specific_file) {
                    execl(tool, tool, "e", "-y", "-idq", opt_ht, opt_p,
                          archive, specific_file, opt_o, (char *)NULL);
                } else {
                    execl(tool, tool, "x", "-y", "-idq", opt_ht, opt_p,
                          archive, opt_o, (char *)NULL);
                }
            } else {
                if (specific_file) {
                    execl(tool, tool, "e", "-y", "-idq", opt_ht,
                          archive, specific_file, opt_o, (char *)NULL);
                } else {
                    execl(tool, tool, "x", "-y", "-idq", opt_ht,
                          archive, opt_o, (char *)NULL);
                }
            }
        } else {
            if (has_pass) {
                execl(tool, tool, "x", "-y", opt_p, archive,
                      opt_o, (char *)NULL);
            } else {
                execl(tool, tool, "x", "-y", archive,
                      opt_o, (char *)NULL);
            }
        }
        _exit(127);
    }

    int status = 0;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        if (code == 0) return YEN_OK;
        if (code == 3 || code == 11) return YEN_ERR_PASSWORD;
    }
    return YEN_ERR_FORMAT;
}

/* ─────────────────────────────────────────────────────────────
 * libarchive extraction
 * ───────────────────────────────────────────────────────────── */

static int copy_block(struct archive *ar, struct archive *aw,
                      uint64_t *written, uint64_t max_extract) {
    const void *buff;
    size_t size;
    la_int64_t offset;

    while (1) {
        int r = archive_read_data_block(ar, &buff, &size, &offset);
        if (r == ARCHIVE_EOF) return ARCHIVE_OK;
        if (r < ARCHIVE_WARN) return r;

        int w = archive_write_data_block(aw, buff, size, offset);
        if (w < ARCHIVE_WARN) return w;

        *written += size;
        if (max_extract > 0 && *written > max_extract) return ARCHIVE_FATAL;
    }
}

static int should_extract_file(const char *pathname, const char *specific) {
    if (!specific || !*specific) return 1;
    if (strcmp(pathname, specific) == 0) return 1;
    const char *b = base_name(pathname);
    if (strcmp(b, specific) == 0) return 1;
    return 0;
}

static yen_status_t extract_one_libarchive(const char *path,
                                           const char *output_dir,
                                           const char *specific_file,
                                           const char *password,
                                           const yen_options_t *opts) {
    struct archive *a = archive_read_new();
    struct archive *ext = archive_write_disk_new();
    struct archive_entry *entry;

    archive_read_support_filter_all(a);
    archive_read_support_format_all(a);
    archive_read_support_format_raw(a);

    if (password && *password) {
        archive_read_add_passphrase(a, password);
    }

    int flags = ARCHIVE_EXTRACT_TIME;
    if (opts->overwrite) flags |= ARCHIVE_EXTRACT_UNLINK;
    if (opts->preserve_perms) flags |= ARCHIVE_EXTRACT_PERM | ARCHIVE_EXTRACT_ACL;
    else flags |= ARCHIVE_EXTRACT_NO_OVERWRITE_NEWER;

    archive_write_disk_set_options(ext, flags);
    archive_write_disk_set_standard_lookup(ext);

    if (archive_read_open_filename(a, path, YEN_BUFFER_SIZE) != ARCHIVE_OK) {
        LOG_E("cannot open: %s", archive_error_string(a));
        archive_read_free(a);
        archive_write_free(ext);
        return YEN_ERR_OPEN;
    }

    while (1) {
        int r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF) break;

        if (r == ARCHIVE_FATAL) {
            const char *err = archive_error_string(a);
            if (err && (strstr(err, "Passphrase") || strstr(err, "encrypted") ||
                        strstr(err, "password"))) {
                archive_read_free(a);
                archive_write_free(ext);
                return YEN_ERR_PASSWORD;
            }
            LOG_E("fatal: %s", err ? err : "unknown");
            archive_read_free(a);
            archive_write_free(ext);
            return YEN_ERR_FORMAT;
        }

        if (r < ARCHIVE_WARN) {
            LOG_W("header: %s", archive_error_string(a));
            g_stats.errors++;
        }

        const char *pathname = archive_entry_pathname(entry);
        if (!pathname) continue;

        if (!is_safe_path(pathname)) {
            LOG_W("unsafe path: %s", pathname);
            g_stats.unsafe_paths++;
            archive_read_data_skip(a);
            continue;
        }

        if (!should_extract_file(pathname, specific_file)) {
            archive_read_data_skip(a);
            g_stats.files_skipped++;
            continue;
        }

        char norm[YEN_MAX_PATH];
        if (normalize_path(pathname, norm, sizeof(norm)) != 0) {
            g_stats.files_skipped++;
            continue;
        }

        char full[YEN_MAX_PATH];
        int nfull = snprintf(full, sizeof(full), "%s/%s", output_dir, norm);
        if (nfull < 0 || (size_t)nfull >= sizeof(full)) {
            g_stats.files_skipped++;
            archive_read_data_skip(a);
            continue;
        }

        if (!path_inside(output_dir, full)) {
            g_stats.unsafe_paths++;
            continue;
        }

        char *dir_copy = xstrdup(full);
        if (!dir_copy) {
            archive_read_free(a);
            archive_write_free(ext);
            return YEN_ERR_MEMORY;
        }
        char *slash = strrchr(dir_copy, '/');
        if (slash) {
            *slash = '\0';
            if (mkdir_p(dir_copy) != 0) {
                LOG_W("mkdir fail: %s", dir_copy);
            } else {
                g_stats.dirs_created++;
            }
        }
        free(dir_copy);

        archive_entry_set_pathname(entry, full);

        int w = archive_write_header(ext, entry);
        if (w < ARCHIVE_OK) {
            g_stats.errors++;
        }

        if (archive_entry_size(entry) > 0) {
            uint64_t written = 0;
            int cr = copy_block(a, ext, &written, opts->max_extract);
            if (cr < ARCHIVE_WARN) {
                g_stats.errors++;
            }
            g_stats.total_bytes += written;
            if (opts->max_extract > 0 &&
                g_stats.total_bytes > opts->max_extract) {
                LOG_E("total exceeds limit");
                archive_read_free(a);
                archive_write_free(ext);
                return YEN_ERR_LIMIT;
            }
        }

        archive_write_finish_entry(ext);
        g_stats.files_extracted++;

        if (!opts->no_progress && g_stats.total_files > 0) {
            strncpy(g_stats.current_file, pathname,
                    sizeof(g_stats.current_file) - 1);
            g_stats.current_file[sizeof(g_stats.current_file) - 1] = '\0';
            render_progress();
        }
    }

    archive_read_close(a);
    archive_read_free(a);
    archive_write_close(ext);
    archive_write_free(ext);
    return YEN_OK;
}

/* ─────────────────────────────────────────────────────────────
 * Dispatcher
 * ───────────────────────────────────────────────────────────── */

static yen_status_t extract_one(const char *path,
                                const char *output_dir,
                                const char *specific_file,
                                const char *password,
                                const yen_options_t *opts) {
    yen_type_t type = detect_type(path);

    if (type == YEN_TYPE_7Z) {
        yen_status_t st = fast_extract_7z(path, output_dir,
                                           specific_file, password, opts);
        if (st == YEN_OK) return st;
        if (st == YEN_ERR_PASSWORD) return st;
        LOG_D("7z tool failed, fallback to libarchive");
        return extract_one_libarchive(path, output_dir, specific_file,
                                       password, opts);
    }

    if (type == YEN_TYPE_RAR || type == YEN_TYPE_RAR5) {
        yen_status_t st = fast_extract_rar(path, output_dir,
                                            specific_file, password, opts);
        if (st == YEN_OK) return st;
        if (st == YEN_ERR_PASSWORD) return st;
        LOG_D("unrar failed, fallback to libarchive");
        return extract_one_libarchive(path, output_dir, specific_file,
                                       password, opts);
    }

    if (type == YEN_TYPE_ZIP) {
        yen_status_t st = fast_extract_zip(path, output_dir,
                                            specific_file, password, opts);
        if (st == YEN_OK) return st;
        if (st == YEN_ERR_PASSWORD) return st;
        LOG_D("unzip failed, fallback to libarchive");
        return extract_one_libarchive(path, output_dir, specific_file,
                                       password, opts);
    }

    return extract_one_libarchive(path, output_dir, specific_file,
                                   password, opts);
}

/* ─────────────────────────────────────────────────────────────
 * Recursive
 * ───────────────────────────────────────────────────────────── */

static void scan_for_nested(const char *dir, int depth, int max_depth) {
    if (depth >= max_depth) return;

    DIR *d = opendir(dir);
    if (!d) return;

    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0) continue;
        if (strcmp(ent->d_name, "..") == 0) continue;

        char full[YEN_MAX_PATH];
        int n = snprintf(full, sizeof(full), "%s/%s", dir, ent->d_name);
        if (n < 0 || (size_t)n >= sizeof(full)) continue;

        if (is_directory(full)) {
            scan_for_nested(full, depth, max_depth);
            continue;
        }

        if (!is_regular_file(full)) continue;

        yen_type_t t = detect_type(full);
        if (is_archive_type(t) || is_compressed_only(t)) {
            queue_push(&g_queue, full, t, depth + 1);
        }
    }
    closedir(d);
}

static yen_status_t extract_nested(const char *path,
                                   const char *output_dir,
                                   const yen_options_t *opts) {
    yen_type_t root_type = detect_type(path);
    queue_push(&g_queue, path, root_type, 0);

    uint64_t processed = 0;
    while (processed < g_queue.count) {
        nested_task_t task;
        pthread_mutex_lock(&g_queue.lock);
        task = g_queue.items[processed++];
        pthread_mutex_unlock(&g_queue.lock);

        char subdir[YEN_MAX_PATH];
        if (task.depth == 0) {
            snprintf(subdir, sizeof(subdir), "%s", output_dir);
        } else {
            const char *b = base_name(task.path);
            char clean[256];
            strncpy(clean, b, sizeof(clean) - 1);
            clean[sizeof(clean) - 1] = '\0';
            char *dot = strrchr(clean, '.');
            if (dot) *dot = '\0';
            for (char *c = clean; *c; c++) {
                if (!isalnum((unsigned char)*c) && *c != '-' && *c != '_') {
                    *c = '_';
                }
            }
            snprintf(subdir, sizeof(subdir), "%s/nested_%s", output_dir, clean);
        }

        if (mkdir_p(subdir) != 0) {
            LOG_W("cannot create: %s", subdir);
            continue;
        }

        LOG_I("[%d] extracting: %s%s%s", task.depth,
              col(C_WHITE), base_name(task.path), rst());

        g_stats.total_files = count_files_in_archive(task.path);
        g_stats.files_extracted = 0;
        g_stats.start_ms = now_ms();

        yen_status_t st = extract_one(task.path, subdir, NULL,
                                       opts->password, opts);

        if (st == YEN_ERR_PASSWORD) {
            char pw_buffer[YEN_MAX_PASSWORD] = {0};
            const char *pw = prompt_password(base_name(task.path),
                                              pw_buffer, sizeof(pw_buffer));
            if (pw) {
                g_stats.files_extracted = 0;
                g_stats.total_bytes = 0;
                g_stats.start_ms = now_ms();
                st = extract_one(task.path, subdir, NULL, pw, opts);
            }
        }

        clear_progress();

        if (st == YEN_OK) {
            g_stats.archives_processed++;
            scan_for_nested(subdir, task.depth, opts->max_depth);
        }
    }

    return YEN_OK;
}

/* ─────────────────────────────────────────────────────────────
 * Banner
 * ───────────────────────────────────────────────────────────── */

static void print_banner(void) {
    if (g_log_quiet) return;

    if (!g_use_color) {
        log_raw("\n");
        log_raw("██╗   ██╗███████╗███╗   ██╗\n");
        log_raw("╚██╗ ██╔╝██╔════╝████╗  ██║\n");
        log_raw(" ╚████╔╝ █████╗  ██╔██╗ ██║\n");
        log_raw("  ╚██╔╝  ██╔══╝  ██║╚██╗██║\n");
        log_raw("   ██║   ███████╗██║ ╚████║\n");
        log_raw("   ╚═╝   ╚══════╝╚═╝  ╚═══╝\n");
        log_raw("\n");
        log_raw("  YenPressor — archive extractor\n");
        log_raw("\n");
        return;
    }

    log_raw("%s", C_CYAN);
    log_raw("██╗   ██╗███████╗███╗   ██╗\n");
    log_raw("╚██╗ ██╔╝██╔════╝████╗  ██║\n");
    log_raw(" ╚████╔╝ █████╗  ██╔██╗ ██║\n");
    log_raw("  ╚██╔╝  ██╔══╝  ██║╚██╗██║\n");
    log_raw("   ██║   ███████╗██║ ╚████║\n");
    log_raw("   ╚═╝   ╚══════╝╚═╝  ╚═══╝\n");
    log_raw("%s", C_RESET);
    log_raw("%s  ──────────────────────────────────%s\n", C_YELLOW, C_RESET);
    log_raw("  %sYenPressor%s  %s·%s  %sfast, safe, streaming%s\n",
            C_WHITE, C_RESET,
            C_YELLOW, C_RESET,
            C_GRAY, C_RESET);
    log_raw("\n");
}

/* ─────────────────────────────────────────────────────────────
 * Summary
 * ───────────────────────────────────────────────────────────── */

static void print_summary(uint64_t start_ms) {
    clear_progress();

    char human[32];
    uint64_t elapsed = now_ms() - start_ms;

    human_size(g_stats.total_bytes, human, sizeof(human));

    const char *lbl = g_use_color ? C_WHITE : "";
    const char *val = g_use_color ? C_YELLOW : "";
    const char *brd = g_use_color ? C_CYAN : "";
    const char *red = g_use_color ? C_RED : "";
    const char *r = g_use_color ? C_RESET : "";
    const char *bl = g_use_color ? C_BOLD : "";

    const char *unsafe_col = (g_stats.unsafe_paths > 0) ? red : val;
    const char *errors_col = (g_stats.errors > 0) ? red : val;

    log_raw("\n%s━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n",
            brd, r);
    log_raw("%s  Summary%s\n", bl, r);
    log_raw("%s━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n",
            brd, r);

    log_raw("  %sArchives%s   %s%lu%s\n",
            lbl, r, val, (unsigned long)g_stats.archives_processed, r);
    log_raw("  %sFiles%s      %s%lu%s\n",
            lbl, r, val, (unsigned long)g_stats.files_extracted, r);
    log_raw("  %sDirs%s       %s%lu%s\n",
            lbl, r, val, (unsigned long)g_stats.dirs_created, r);
    log_raw("  %sSize%s       %s%s%s\n",
            lbl, r, val, human, r);
    log_raw("  %sSkipped%s    %s%lu%s\n",
            lbl, r, val, (unsigned long)g_stats.files_skipped, r);
    log_raw("  %sUnsafe%s     ", lbl, r);
    log_raw("%s%lu%s\n", unsafe_col,
            (unsigned long)g_stats.unsafe_paths, r);
    log_raw("  %sErrors%s     ", lbl, r);
    log_raw("%s%lu%s\n", errors_col,
            (unsigned long)g_stats.errors, r);
    log_raw("  %sElapsed%s    %s%.2fs%s\n",
            lbl, r, val, elapsed / 1000.0, r);

    if (elapsed > 0 && g_stats.total_bytes > 0) {
        double rate = (double)g_stats.total_bytes / (elapsed / 1000.0);
        char rate_h[32];
        human_rate(rate, rate_h, sizeof(rate_h));
        log_raw("  %sSpeed%s      %s%s%s\n", lbl, r, val, rate_h, r);
    }

    log_raw("%s━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n",
            brd, r);
}

/* ─────────────────────────────────────────────────────────────
 * Usage
 * ───────────────────────────────────────────────────────────── */

static void print_usage(FILE *out) {
    fprintf(out,
"Usage: YenPressor [OPTIONS] ARCHIVE [ARCHIVE...]\n"
"\n"
"Options:\n"
"  -o, --output DIR    Output directory (default: ./extracted)\n"
"  -l, --list          List contents only\n"
"  -f, --file NAME     Extract only this file\n"
"  -p, --password PASS Password (optional, will prompt if needed)\n"
"  -r, --recursive     Extract nested archives\n"
"  -d, --depth N       Max recursion depth (default: 10)\n"
"  -m, --max-size SIZE Max extraction size (default: unlimited)\n"
"      --preserve      Preserve permissions\n"
"      --overwrite     Overwrite existing files\n"
"      --no-color      Disable colors\n"
"      --no-progress   Disable progress bar\n"
"  -v, --verbose       Verbose output\n"
"  -q, --quiet         Quiet (errors only)\n"
"  -h, --help          Show this help\n"
"  -V, --version       Show version\n"
"\n"
"Examples:\n"
"  YenPressor file.zip\n"
"  YenPressor file.zip -o /tmp/out\n"
"  YenPressor file.zip -l\n"
"  YenPressor file.7z -r\n"
"  YenPressor file.rar\n"
"  YenPressor encrypted.zip\n"
"  YenPressor encrypted.7z -p mypassword\n"
"\n"
"For maximum speed, install:\n"
"  sudo apt install p7zip-full unrar unzip\n"
"\n");
}

static void print_version(void) {
    printf("YenPressor v%s\n", YEN_VERSION);
    printf("libarchive: %s\n", archive_version_details());
    printf("cpu cores detected: %d\n", g_cpu_cores);
}

/* ─────────────────────────────────────────────────────────────
 * Args
 * ───────────────────────────────────────────────────────────── */

enum {
    OPT_PRESERVE = 1000,
    OPT_OVERWRITE,
    OPT_NOCOLOR,
    OPT_NOPROGRESS,
};

static int parse_args(int argc, char **argv, yen_options_t *opts) {
    static struct option long_opts[] = {
        {"output",       required_argument, 0, 'o'},
        {"list",         no_argument,       0, 'l'},
        {"file",         required_argument, 0, 'f'},
        {"password",     required_argument, 0, 'p'},
        {"recursive",    no_argument,       0, 'r'},
        {"depth",        required_argument, 0, 'd'},
        {"max-size",     required_argument, 0, 'm'},
        {"preserve",     no_argument,       0, OPT_PRESERVE},
        {"overwrite",    no_argument,       0, OPT_OVERWRITE},
        {"no-color",     no_argument,       0, OPT_NOCOLOR},
        {"no-progress",  no_argument,       0, OPT_NOPROGRESS},
        {"verbose",      no_argument,       0, 'v'},
        {"quiet",        no_argument,       0, 'q'},
        {"help",         no_argument,       0, 'h'},
        {"version",      no_argument,       0, 'V'},
        {0, 0, 0, 0}
    };

    opts->output_dir = "./extracted";
    opts->max_depth = YEN_DEFAULT_DEPTH;
    opts->max_extract = 0;

    int c;
    while ((c = getopt_long(argc, argv, "o:lf:p:rd:m:vqhV",
                            long_opts, NULL)) != -1) {
        switch (c) {
            case 'o': opts->output_dir = optarg; break;
            case 'l': opts->list_only = 1; break;
            case 'f': opts->specific_file = optarg; break;
            case 'p': opts->password = optarg; break;
            case 'r': opts->recursive = 1; break;
            case 'd':
                opts->max_depth = atoi(optarg);
                if (opts->max_depth < 1) opts->max_depth = 1;
                if (opts->max_depth > YEN_MAX_DEPTH) opts->max_depth = YEN_MAX_DEPTH;
                break;
            case 'm':
                opts->max_extract = parse_size(optarg);
                break;
            case 'v': opts->verbose = 1; break;
            case 'q': opts->quiet = 1; break;
            case OPT_PRESERVE: opts->preserve_perms = 1; break;
            case OPT_OVERWRITE: opts->overwrite = 1; break;
            case OPT_NOCOLOR: opts->no_color = 1; break;
            case OPT_NOPROGRESS: opts->no_progress = 1; break;
            case 'h': print_usage(stdout); exit(0);
            case 'V': print_version(); exit(0);
            default: print_usage(stderr); return -1;
        }
    }

    if (optind >= argc) {
        print_usage(stderr);
        return -1;
    }
    return optind;
}

/* ─────────────────────────────────────────────────────────────
 * Process
 * ───────────────────────────────────────────────────────────── */

static yen_status_t process_archive(const char *path, yen_options_t *opts) {
    if (!file_exists(path)) {
        LOG_E("not found: %s", path);
        return YEN_ERR_OPEN;
    }

    yen_type_t type = detect_type(path);
    if (type == YEN_TYPE_UNKNOWN) {
        LOG_E("unknown archive format: %s", path);
        return YEN_ERR_FORMAT;
    }

    char human[32];
    human_size(file_size(path), human, sizeof(human));

    LOG_I("%s%s%s %s(%s, %s%s%s)%s",
          col(C_WHITE), base_name(path), rst(),
          col(C_GRAY), human, col(type_color(type)),
          type_name(type), col(C_GRAY), rst());

    if (opts->list_only) {
        return list_archive(path);
    }

    if (mkdir_p(opts->output_dir) != 0) {
        LOG_E("cannot create output dir: %s", opts->output_dir);
        return YEN_ERR_WRITE;
    }

    if (opts->verbose) {
        uint64_t free_space = disk_free(opts->output_dir);
        if (free_space > 0) {
            char free_h[32];
            human_size(free_space, free_h, sizeof(free_h));
            LOG_D("free disk space: %s", free_h);
        }
    }

    if (opts->recursive) {
        return extract_nested(path, opts->output_dir, opts);
    }

    g_stats.total_files = opts->no_progress ? 0 : count_files_in_archive(path);
    g_stats.files_extracted = 0;
    g_stats.start_ms = now_ms();

    yen_status_t st = extract_one(path, opts->output_dir,
                                   opts->specific_file,
                                   opts->password, opts);

    if (st == YEN_ERR_PASSWORD && !opts->password) {
        char pw_buffer[YEN_MAX_PASSWORD] = {0};
        const char *pw = prompt_password(base_name(path),
                                          pw_buffer, sizeof(pw_buffer));
        if (pw) {
            g_stats.files_extracted = 0;
            g_stats.total_bytes = 0;
            g_stats.start_ms = now_ms();
            st = extract_one(path, opts->output_dir,
                             opts->specific_file, pw, opts);
            if (st == YEN_ERR_PASSWORD) {
                LOG_E("wrong password");
            }
        }
    } else if (st == YEN_ERR_PASSWORD && opts->password) {
        LOG_E("wrong password (from -p)");
    }

    clear_progress();

    if (st == YEN_OK) {
        g_stats.archives_processed++;
    }

    return st;
}

/* ─────────────────────────────────────────────────────────────
 * Main
 * ───────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    uint64_t start_ms = now_ms();
    yen_options_t opts = {0};

    pthread_mutex_init(&g_stats.lock, NULL);

    g_cpu_cores = detect_cpu_cores();

    if (!isatty(STDOUT_FILENO) || !isatty(STDERR_FILENO)) {
        g_use_color = 0;
    }
    if (getenv("NO_COLOR")) g_use_color = 0;

    int first_archive = parse_args(argc, argv, &opts);
    if (first_archive < 0) return 1;

    if (opts.no_color) g_use_color = 0;
    if (opts.verbose) g_log_level = LOG_DEBUG;
    if (opts.quiet) { g_log_quiet = 1; g_log_level = LOG_WARN; }

    print_banner();

    g_stats.start_ms = start_ms;

    queue_init(&g_queue);

    int exit_code = 0;
    for (int i = first_archive; i < argc; i++) {
        yen_status_t st = process_archive(argv[i], &opts);
        if (st != YEN_OK) {
            exit_code = 1;
        }
    }

    queue_free(&g_queue);

    if (!opts.quiet && !opts.list_only) {
        print_summary(start_ms);
    }

    pthread_mutex_destroy(&g_stats.lock);
    return exit_code;
}
