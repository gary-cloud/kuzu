#pragma once

#include "binder/binder.h"
#include "binder/binder.h"
#include "common/case_insensitive_map.h"
#include "common/copy_constructors.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/table_function.h"
#include "function/table/scan_file_function.h"

#include "arrow/api.h"
#include "arrow/filesystem/api.h"

#include "graphar/api/high_level_reader.h"

namespace kuzu {
namespace graphar_extension {

struct GrapharScanFunction {
    static constexpr const char* name = "GRAPHAR_SCAN";

    static function::function_set getFunctionSet();
};

using column_name_idx_map_t = std::unordered_map<std::string, uint64_t>;

class KuzuColumnInfo {
public:
    explicit KuzuColumnInfo(std::vector<std::string> columnNames);
    DELETE_COPY_DEFAULT_MOVE(KuzuColumnInfo);

    uint64_t getFieldIdx(std::string fieldName) const;

private:
    column_name_idx_map_t colNameToIdx;
    std::vector<std::string> colNames;
};

struct GrapharScanBindData final : function::ScanFileBindData {
    std::shared_ptr<graphar::GraphInfo> graph_info;
    std::shared_ptr<KuzuColumnInfo> column_info;
    std::string table_name;
    std::vector<std::string> column_names;
    
    uint64_t getFieldIdx(std::string fieldName) const { return column_info->getFieldIdx(fieldName); }

    GrapharScanBindData(binder::expression_vector columns, common::FileScanInfo fileScanInfo, main::ClientContext* context,
        std::shared_ptr<graphar::GraphInfo> graph_info, std::string table_name, std::vector<std::string> column_names);

    GrapharScanBindData(const GrapharScanBindData& other) 
        : ScanFileBindData(other),
          graph_info(other.graph_info),
          column_info(other.column_info),
          table_name(other.table_name) {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<GrapharScanBindData>(*this);
    }
};

struct GrapharScanSharedState : public function::TableFuncSharedState {
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybe_vertices_collection;
    graphar::VertexIter vertex_iter;
    size_t vertices_count;

    GrapharScanSharedState(graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybeVerticesCollection, 
        graphar::VertexIter vertex_iter);
};

// Functions and structs exposed for use
std::unique_ptr<function::TableFuncSharedState> initGrapharScanSharedState(
    const function::TableFuncInitSharedStateInput& input);

std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* context,
    const function::TableFuncBindInput* input);

common::offset_t tableFunc(const function::TableFuncInput& input,
    function::TableFuncOutput& output);

} // namespace graphar_extension
} // namespace kuzu
