#pragma once

#include "function/function.h"
#include "function/table/bind_data.h"

namespace kuzu {
namespace graphar_extension {

struct GrapharExportFunction {
    static constexpr const char* name = "COPY_GRAPHAR";

    static function::function_set getFunctionSet();
};

} // namespace graphar_extension
} // namespace kuzu
