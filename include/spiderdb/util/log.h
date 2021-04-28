//
// Created by chungphb on 28/4/21.
//

#pragma once

#include <seastar/util/log.hh>

namespace spiderdb {

extern seastar::logger spiderdb_logger;

#ifdef SPIDERDB_TESTING

#define SPIDERDB_LOGGER_ERROR(...) spiderdb_logger.error(__VA_ARGS__)
#define SPIDERDB_LOGGER_WARN(...) spiderdb_logger.warn(__VA_ARGS__)
#define SPIDERDB_LOGGER_INFO(...) spiderdb_logger.info(__VA_ARGS__)
#define SPIDERDB_LOGGER_DEBUG(...) spiderdb_logger.debug(__VA_ARGS__)
#define SPIDERDB_LOGGER_TRACE(...) spiderdb_logger.trace(__VA_ARGS__)

#else

#define SPIDERDB_LOGGER_ERROR(...) if (0)
#define SPIDERDB_LOGGER_WARN(...) if (0)
#define SPIDERDB_LOGGER_INFO(...) if (0)
#define SPIDERDB_LOGGER_DEBUG(...) if (0)
#define SPIDERDB_LOGGER_TRACE(...) if (0)

#endif

}