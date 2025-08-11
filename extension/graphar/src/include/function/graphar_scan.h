#pragma once

#include <functional>
#include <atomic> 

#include "main/client_context.h"
#include "binder/binder.h"
#include "binder/binder.h"
#include "common/types/types.h"
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

// The setter for each column: Write the value of the current vertexIter in this column to the corresponding ValueVector of output at row.
using ColumnSetter = std::function<void(graphar::VertexIter&, function::TableFuncOutput&, kuzu::common::idx_t)>;
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
    std::vector<kuzu::common::LogicalType> column_types;
    std::vector<ColumnSetter> column_setters;
    uint64_t max_threads;
    
    uint64_t getFieldIdx(std::string fieldName) const { return column_info->getFieldIdx(fieldName); }

    GrapharScanBindData(binder::expression_vector columns, common::FileScanInfo fileScanInfo, main::ClientContext* context,
        std::shared_ptr<graphar::GraphInfo> graph_info, std::string table_name, std::vector<std::string> column_names,
        std::vector<kuzu::common::LogicalType> column_types);

    GrapharScanBindData(const GrapharScanBindData& other) 
        : ScanFileBindData(other),
          graph_info(other.graph_info),
          column_info(other.column_info),
          table_name(other.table_name), 
          column_types(copyVector(other.column_types)),
          column_setters(other.column_setters),
          max_threads(other.max_threads) {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<GrapharScanBindData>(*this);
    }
};

struct GrapharScanSharedState : public function::TableFuncSharedState {
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybe_vertices_collection;
    std::atomic<size_t> next_index{0};
    size_t vertices_count;
    size_t batch_size;

    GrapharScanSharedState(graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybeVerticesCollection, uint64_t max_threads);
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
