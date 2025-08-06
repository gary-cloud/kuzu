#include "function/graphar_scan.h"

#include "common/exception/not_implemented.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

std::unique_ptr<TableFuncSharedState> iniGrapharScanSharedState(
    const TableFuncInitSharedStateInput& input) {
    throw NotImplementedException{"GrapharScanFunction::initGrapharScanSharedState is not implemented yet."};
}

offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    throw NotImplementedException{"GrapharScanFunction::tableFunc is not implemented yet."};
}

function_set GrapharScanFunction::getFunctionSet() {
    throw NotImplementedException{"GrapharScanFunction::getFunctionSet is not implemented yet."};
}

} // namespace graphar_extension
} // namespace kuzu
