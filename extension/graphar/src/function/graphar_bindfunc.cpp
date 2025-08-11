#include "function/graphar_scan.h"

#include "common/exception/not_implemented.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

template<typename T>
ColumnSetter makeTypedSetter(uint64_t fieldIdx, std::string colName) {
    return [fieldIdx, colName = std::move(colName)](graphar::VertexIter& it, function::TableFuncOutput& output, kuzu::common::idx_t row) {
        auto res = it.property<T>(colName);
        auto &vec = output.dataChunk.getValueVectorMutable(fieldIdx);
        vec.setValue(row, res.value());
    };
}

static const std::unordered_map<LogicalTypeID, std::function<ColumnSetter(uint64_t, std::string)>> setterFactory = {
    { LogicalTypeID::INT64,   [](uint64_t idx, std::string col){ return makeTypedSetter<int64_t>(idx, std::move(col)); } },
    { LogicalTypeID::INT32,   [](uint64_t idx, std::string col){ return makeTypedSetter<int32_t>(idx, std::move(col)); } },
    { LogicalTypeID::DOUBLE,  [](uint64_t idx, std::string col){ return makeTypedSetter<double>(idx, std::move(col)); } },
    { LogicalTypeID::FLOAT,   [](uint64_t idx, std::string col){ return makeTypedSetter<float>(idx, std::move(col)); } },
    { LogicalTypeID::STRING,  [](uint64_t idx, std::string col){ return makeTypedSetter<std::string>(idx, std::move(col)); } },
    { LogicalTypeID::BOOL,    [](uint64_t idx, std::string col){ return makeTypedSetter<bool>(idx, std::move(col)); } },
    // DATE / TIMESTAMP may need custom conversion logic — handle explicitly if graphar returns non-native types.
};

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

static void autoDetectSchema([[maybe_unused]] main::ClientContext* context, std::shared_ptr<graphar::GraphInfo> graph_info, std::string table_name,
    std::vector<LogicalType>& types, std::vector<std::string>& names) {
    auto vertex_info = graph_info->GetVertexInfo(table_name);
    if (!vertex_info) {
        throw BinderException("GraphAr's Type " + table_name + " does not exist.");
    }

    // Construct the types and names from the vertex info.
    for (auto& property_group : vertex_info->GetPropertyGroups()) {
        for (const auto& property : property_group->GetProperties()) {
            names.push_back(property.name);
            types.push_back(GrapharTypeToKuzuTypeFunc(property.type->id()));
        }
    }
}

GrapharScanBindData::GrapharScanBindData(binder::expression_vector columns, common::FileScanInfo fileScanInfo, main::ClientContext* context,
    std::shared_ptr<graphar::GraphInfo> graph_info, std::string table_name, std::vector<std::string> column_names, std::vector<kuzu::common::LogicalType> column_types)
    : ScanFileBindData{std::move(columns), 0 /* numRows */, std::move(fileScanInfo), context},
        graph_info{std::move(graph_info)}, column_info{std::make_shared<KuzuColumnInfo>(column_names)},
        table_name{std::move(table_name)}, column_names{std::move(column_names)}, column_types{std::move(column_types)} {
            this->column_setters.reserve(this->column_names.size());
            for (size_t i = 0; i < this->column_names.size(); ++i) {
                auto typeID = this->column_types[i].getLogicalTypeID();
                uint64_t fieldIdx = getFieldIdx(this->column_names[i]);
                auto it = setterFactory.find(typeID);
                if (it == setterFactory.end()) {
                    throw NotImplementedException{"Unsupported column type in GrapharScan bind: " + std::to_string((int)typeID)};
                }
                this->column_setters.push_back(it->second(fieldIdx, this->column_names[i]));
            }
            this->max_threads = context->getMaxNumThreadForExec();
        }

KuzuColumnInfo::KuzuColumnInfo(std::vector<std::string> column_names) {
    this->colNames = std::move(column_names);
    idx_t colIdx = 0;
    for (auto& colName : this->colNames) {
        colNameToIdx.insert({colName, colIdx++});
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

    std::vector<LogicalType> column_types;
    std::vector<std::string> column_names;

    autoDetectSchema(context, graph_info, table_name, column_types, column_names);

    // TODO: If expected column names and types are provided, use them.
    // if (!scanInput->expectedColumnNames.empty()) {
    //     column_types = copyVector(scanInput->expectedColumnTypes);
    //     column_names = scanInput->expectedColumnNames;
    // } else {
    //     autoDetectSchema(context, graph_info, table_name, column_types, column_names);
    // }

    KU_ASSERT(column_types.size() == column_names.size());
    
    column_names =
        TableFunction::extractYieldVariables(column_names, input->yieldVariables);
    auto columns = input->binder->createVariables(column_names, column_types);
    return std::make_unique<GrapharScanBindData>(std::move(columns), scanInput->fileScanInfo.copy(), context,
        std::move(graph_info), std::move(table_name), std::move(column_names), std::move(column_types));
}

} // namespace graphar_extension
} // namespace kuzu
