#include "function/graphar_export.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace graphar_extension {

void initSharedState(ExportFuncSharedState& sharedState, main::ClientContext& context,
    const ExportFuncBindData& bindData) {
    sharedState.init(context, bindData);
}

std::shared_ptr<ExportFuncSharedState> createSharedStateFunc() {
    return std::make_shared<ExportGrapharSharedState>();
}

std::unique_ptr<ExportFuncLocalState> initLocalState(main::ClientContext&,
    const ExportFuncBindData&, std::vector<bool>) {
    return std::make_unique<ExportGrapharLocalState>();
}

void sinkFunc(ExportFuncSharedState&, ExportFuncLocalState& localState,
    const ExportFuncBindData& bindData, std::vector<std::shared_ptr<ValueVector>> inputVectors) {
    auto& grapharLocalState = localState.cast<ExportGrapharLocalState>();
}

void combineFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState) {
    auto& grapharSharedState = sharedState.cast<ExportGrapharSharedState>();
    auto& grapharLocalState = localState.cast<ExportGrapharLocalState>();
}

void finalizeFunc(ExportFuncSharedState& sharedState) {
    auto& grapharSharedState = sharedState.cast<ExportGrapharSharedState>();
}

function_set GrapharExportFunction::getFunctionSet() {
    function_set functionSet;
    auto exportFunc = std::make_unique<ExportFunction>(name);
    exportFunc->bind = bindFunc;
    exportFunc->initLocalState = initLocalState;
    exportFunc->createSharedState = createSharedStateFunc;
    exportFunc->initSharedState = initSharedState;
    exportFunc->sink = sinkFunc;
    exportFunc->combine = combineFunc;
    exportFunc->finalize = finalizeFunc;
    functionSet.push_back(std::move(exportFunc));
    return functionSet;
}

} // namespace graphar_extension
} // namespace kuzu
