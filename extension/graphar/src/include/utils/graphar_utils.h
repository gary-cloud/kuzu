#pragma once

#include "common/string_utils.h"
#include "common/types/types.h"
#include "common/exception/not_implemented.h"
#include "graphar/types.h"

namespace kuzu {
namespace graphar_extension {

using namespace kuzu::common;
using namespace graphar;

/**
 * Checks whether a string ends with a given suffix.
 */
inline bool ends_with(const std::string& s, const std::string& suffix);

/**
 * parse_is_edge
 *  - Input:  A file path string (e.g., "xxx/ldbc/parquet/person.vertex.yml")
 *  - Return: true  -> if the file is an edge schema (.edge.yml / .edge.yaml)
 *            false -> if the file is a vertex schema (.vertex.yml / .vertex.yaml)
 *  - Exception: Throws std::invalid_argument if the file name does not match
 *               either vertex or edge schema suffixes.
 *
 *  This function performs case-insensitive matching by converting the file name
 *  to uppercase via common::StringUtils::getUpper. It supports both '/' and '\\'
 *  as valid path separators.
 */
inline bool parse_is_edge(const std::string& path);

/**
 * tryParseEdgeTableName
 *  - Input:  A table name string that may represent an edge relationship
 *  - Output: If parsing succeeds, sets src, edge, dst references to the parsed parts
 *  - Return: true  -> if the table name was successfully parsed as "src.edge.dst" format
 *            false -> if the table name does not match the expected format
 *
 * This function attempts to parse a table name using multiple separator characters
 * in order: '.', ':', '_'. It expects exactly 3 non-empty parts in the format:
 *   [source_vertex_label][separator][edge_label][separator][destination_vertex_label]
 *
 * Examples of valid inputs:
 *   - "person.knows.person"   -> src="person", edge="knows", dst="person"
 *   - "user:follows:user"     -> src="user", edge="follows", dst="user"
 *   - "page_likes_page"       -> src="page", edge="likes", dst="page"
 */
inline bool tryParseEdgeTableName(const std::string& table_name, std::string& src,
    std::string& edge, std::string& dst);

/*
 * Convert GraphAr data type to Kuzu logical type.
 */
LogicalType grapharTypeToKuzuType(std::shared_ptr<graphar::DataType> type);

/*
 * Convert Kuzu logical type to GraphAr data type.
 */
graphar::Type kuzuTypeToGrapharType(const LogicalType& type);

} // namespace graphar_extension
} // namespace kuzu
