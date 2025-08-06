#include "function/graphar_scan.h"

#include "common/exception/not_implemented.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    throw NotImplementedException{"GrapharScanFunction::bindFunc is not implemented yet."};
}

} // namespace graphar_extension
} // namespace kuzu
