#include "function/graphar_scan.h"

#include "common/exception/not_implemented.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

// static std::unordered_map<graphar::Type, LogicalType> GrapharTypeToKuzuType = {
//     {graphar::Type::BOOL, LogicalType::BOOL()},
//     {graphar::Type::INT64, LogicalType::INT64()},
//     {graphar::Type::INT32, LogicalType::INT32()},
//     {graphar::Type::FLOAT, LogicalType::FLOAT()},
//     {graphar::Type::STRING, LogicalType::STRING()},
//     {graphar::Type::DOUBLE, LogicalType::DOUBLE()},
//     {graphar::Type::DATE, LogicalType::DATE()},
//     {graphar::Type::TIMESTAMP, LogicalType::TIMESTAMP()},
// };

static LogicalType GrapharTypeToKuzuTypeFunc(graphar::Type type) {
    switch (type) {
        case graphar::Type::BOOL:
            return LogicalType::BOOL();
        case graphar::Type::INT64:
            return LogicalType::INT64();
        case graphar::Type::INT32:
            return LogicalType::INT32();
        case graphar::Type::FLOAT:
            return LogicalType::FLOAT();
        case graphar::Type::STRING:
            return LogicalType::STRING();
        case graphar::Type::DOUBLE:
            return LogicalType::DOUBLE();
        case graphar::Type::DATE:
            return LogicalType::DATE();
        case graphar::Type::TIMESTAMP:
            return LogicalType::TIMESTAMP();
        default:
            throw NotImplementedException{"GraphAr's Type " + std::to_string(static_cast<int>(type)) + " is not implemented."};
    }
}

static void autoDetectSchema(main::ClientContext* context, std::shared_ptr<graphar::GraphInfo> graph_info, std::string table_name,
    std::vector<LogicalType>& types, std::vector<std::string>& names) {
    auto vertex_info = graph_info->GetVertexInfo(table_name);
    if (!vertex_info) {
        throw BinderException("GraphAr's Type " + table_name + " does not exist.");
    }

    // TODO: Construct the types and names from the vertex info.
    // for (auto& property_group : vertex_info->GetPropertyGroups()) {
    //     const auto& property = property_group->GetProperties().at(0);
    //     names.push_back(property.name);
    //     types.push_back(GrapharTypeToKuzuTypeFunc(property.type->id()));
    // }

    names = {"id", "firstName", "lastName", "gender"};
    // types = {LogicalType::INT64(), LogicalType::STRING(), LogicalType::STRING(), LogicalType::STRING()};
    types.push_back(LogicalType::INT64());
    types.push_back(LogicalType::STRING());
    types.push_back(LogicalType::STRING());
    types.push_back(LogicalType::STRING());
}

GrapharScanBindData::GrapharScanBindData(binder::expression_vector columns, common::FileScanInfo fileScanInfo, main::ClientContext* context,
    std::shared_ptr<graphar::GraphInfo> graph_info, std::string table_name, std::vector<std::string> column_names)
    : ScanFileBindData{std::move(columns), 0 /* numRows */, std::move(fileScanInfo), context},
        graph_info{std::move(graph_info)}, column_info{std::make_shared<KuzuColumnInfo>(column_names)},
        table_name{std::move(table_name)}, column_names{std::move(column_names)} {}

KuzuColumnInfo::KuzuColumnInfo(std::vector<std::string> columnNames) {
    this->colNames = std::move(columnNames);
    idx_t colIdx = 0;
    for (auto& columnName : this->colNames) {
        colNameToIdx.insert({columnName, colIdx++});
    }
}

uint64_t KuzuColumnInfo::getFieldIdx(std::string fieldName) const {
    // For a small number of keys, probing a vector is faster than lookups in an unordered_map.
    if (colNames.size() < 24) {
        auto iter = std::find(colNames.begin(), colNames.end(), fieldName);
        if (iter != colNames.end()) {
            return iter - colNames.begin();
        }
    } else {
        auto itr = colNameToIdx.find(fieldName);
        if (itr != colNameToIdx.end()) {
            return itr->second;
        }
    }
    // From and to are case-insensitive for backward compatibility.
    if (StringUtils::caseInsensitiveEquals(fieldName, "from")) {
        return colNameToIdx.at("from");
    } else if (StringUtils::caseInsensitiveEquals(fieldName, "to")) {
        return colNameToIdx.at("to");
    }
    return UINT64_MAX;
}

std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto scanInput = ku_dynamic_cast<ExtraScanTableFuncBindInput*>(input->extraInput.get());
    std::string absolute_path = scanInput->fileScanInfo.getFilePath(0);
    std::string table_name = scanInput->fileScanInfo.options.at("table_name").strVal;

    // Load graph info from the file path
    auto graph_info = graphar::GraphInfo::Load(absolute_path).value();
    if (!graph_info) {
        throw BinderException("GraphAr's GraphInfo could not be loaded from " + absolute_path);
    }
    auto maybe_vertices_collection = graphar::VerticesCollection::Make(graph_info, table_name);
    if (!maybe_vertices_collection) {
        throw BinderException("GraphAr's VerticesCollection could not be created for " + table_name);
    }
    auto vertexIter = maybe_vertices_collection.value()->begin();

    std::vector<LogicalType> columnTypes;
    std::vector<std::string> columnNames;

    autoDetectSchema(context, graph_info, table_name, columnTypes, columnNames);

    // TODO
    // if (!scanInput->expectedColumnNames.empty()) {
    //     columnTypes = copyVector(scanInput->expectedColumnTypes);
    //     columnNames = scanInput->expectedColumnNames;
    // } else {
    //     autoDetectSchema(context, graph_info, table_name, columnTypes, columnNames);
    // }
    KU_ASSERT(columnTypes.size() == columnNames.size());
    
    columnNames =
        TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<GrapharScanBindData>(std::move(columns), scanInput->fileScanInfo.copy(), context,
        std::move(graph_info), std::move(table_name), std::move(columnNames));
    
    // throw NotImplementedException{"GrapharScanFunction::bindFunc is not implemented yet."};
}

} // namespace graphar_extension
} // namespace kuzu
