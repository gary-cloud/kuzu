#pragma once

#include "binder/binder.h"
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

struct GrapharScanBindData final : function::ScanFileBindData {
};

// Functions and structs exposed for use
std::unique_ptr<function::TableFuncSharedState> initGrapharScanSharedState(
    const function::TableFuncInitSharedStateInput& input);

common::offset_t tableFunc(const function::TableFuncInput& input,
    function::TableFuncOutput& output);

} // namespace graphar_extension
} // namespace kuzu
