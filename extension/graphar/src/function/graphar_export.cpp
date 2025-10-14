#include "graphar_export.h"

#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffer_writer.h"
#include "function/export/export_function.h"
#include "main/client_context.h"
#include "common/exception/not_implemented.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace graphar_extension {

struct ExportGrapharBindData : public ExportFuncBindData {
    ExportGrapharBindData(std::vector<std::string> columnNames, const std::string& fileName)
        : ExportFuncBindData(std::move(columnNames), fileName) {}
    ExportGrapharBindData(std::vector<std::string> columnNames, std::vector<LogicalType> columnTypes,
        std::string fileName)
        : ExportFuncBindData(std::move(columnNames), std::move(fileName)) {
        setDataType(std::move(columnTypes));
    }

    std::unique_ptr<ExportFuncBindData> copy() const override {
        return std::make_unique<ExportGrapharBindData>(columnNames, LogicalType::copy(types),
            fileName);
    }
};

struct ExportGrapharSharedState : public ExportFuncSharedState {
    void init(main::ClientContext& context, const ExportFuncBindData& bindData) override {
        throw NotImplementedException("GraphAr export is not implemented.");
    }
};

struct ExportGrapharLocalState : public ExportFuncLocalState {
};

static std::unique_ptr<ExportFuncBindData> bindFunc(ExportFuncBindInput& bindInput) {
    return std::make_unique<ExportGrapharBindData>(bindInput.columnNames, bindInput.filePath);
}

static void initSharedState(ExportFuncSharedState& sharedState, main::ClientContext& context,
    const ExportFuncBindData& bindData) {
    sharedState.init(context, bindData);
}

static std::shared_ptr<ExportFuncSharedState> createSharedStateFunc() {
    return std::make_shared<ExportGrapharSharedState>();
}

static std::unique_ptr<ExportFuncLocalState> initLocalState(main::ClientContext&,
    const ExportFuncBindData&, std::vector<bool>) {
    return std::make_unique<ExportGrapharLocalState>();
}

static void sinkFunc(ExportFuncSharedState&, ExportFuncLocalState& localState,
    const ExportFuncBindData& bindData, std::vector<std::shared_ptr<ValueVector>> inputVectors) {
    auto& grapharLocalState = localState.cast<ExportGrapharLocalState>();
}

static void combineFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState) {
    auto& grapharSharedState = sharedState.cast<ExportGrapharSharedState>();
    auto& grapharLocalState = localState.cast<ExportGrapharLocalState>();
}

static void finalizeFunc(ExportFuncSharedState& sharedState) {
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
