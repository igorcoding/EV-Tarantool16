#ifndef _LOG_H_
#define _LOG_H_

#define MAX_LOG_LEVEL 4
#define _LOG_NONE  0
#define _LOG_ERROR 1
#define _LOG_WARN  2
#define _LOG_INFO  3
#define _LOG_DEBUG 4

#ifndef _log
#define _log(level, fmt, ...)   do{ \
    if (level == _LOG_NONE) break; \
    switch (level) { \
        case _LOG_ERROR: \
            fprintf(stderr, "[ERROR] %s:%d: ", __FILE__, __LINE__); \
            break; \
        case _LOG_WARN: \
            fprintf(stderr, "[WARN] %s:%d: ", __FILE__, __LINE__); \
            break; \
        case _LOG_INFO: \
            fprintf(stderr, "[INFO] %s:%d: ", __FILE__, __LINE__); \
            break; \
        case _LOG_DEBUG: \
        default: \
            fprintf(stderr, "[DEBUG] %s:%d: ", __FILE__, __LINE__); \
            break; \
    } \
	fprintf(stderr, fmt, ##__VA_ARGS__); \
	if (fmt[strlen(fmt) - 1] != 0x0a) { fprintf(stderr, "\n"); } \
} while(0)

#define _log_error(fmt, ...) _log(_LOG_ERROR, fmt, ##__VA_ARGS__)
#define _log_warn(fmt, ...)  _log(_LOG_WARN,  fmt, ##__VA_ARGS__)
#define _log_info(fmt, ...)  _log(_LOG_INFO,  fmt, ##__VA_ARGS__)
#define _log_debug(fmt, ...) _log(_LOG_DEBUG, fmt, ##__VA_ARGS__)

#define log(max_level, level, fmt, ...) do { \
    if (level <= max_level) { \
        _log(level, fmt, ##__VA_ARGS__); \
    } \
} while(0)

#define log_error(max_level, fmt, ...) log(max_level, _LOG_ERROR, fmt, ##__VA_ARGS__)
#define log_warn(max_level, fmt, ...)  log(max_level, _LOG_WARN,  fmt, ##__VA_ARGS__)
#define log_info(max_level, fmt, ...)  log(max_level, _LOG_INFO,  fmt, ##__VA_ARGS__)
#define log_debug(max_level, fmt, ...) log(max_level, _LOG_DEBUG, fmt, ##__VA_ARGS__)
#endif

#endif // _LOG_H_
