#pragma once

#include "common/exception/not_implemented.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "function/export/export_function.h"
#include "function/function.h"
#include "function/table/bind_data.h"
#include "main/client_context.h"

namespace kuzu {
namespace graphar_extension {

using namespace kuzu::function;
using namespace kuzu::common;

struct GrapharExportFunction {
    static constexpr const char* name = "COPY_GRAPHAR";

    static function_set getFunctionSet();
};

struct ExportGrapharBindData : public ExportFuncBindData {
    ExportGrapharBindData(std::vector<std::string> columnNames, const std::string& fileName);

    ExportGrapharBindData(std::vector<std::string> columnNames,
        std::vector<LogicalType> columnTypes, std::string fileName);

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

struct ExportGrapharLocalState : public ExportFuncLocalState {};

std::unique_ptr<ExportFuncBindData> bindFunc(ExportFuncBindInput& bindInput);

void initSharedState(ExportFuncSharedState& sharedState, main::ClientContext& context,
    const ExportFuncBindData& bindData);

std::shared_ptr<ExportFuncSharedState> createSharedStateFunc();

std::unique_ptr<ExportFuncLocalState> initLocalState(main::ClientContext&,
    const ExportFuncBindData&, std::vector<bool>);

void sinkFunc(ExportFuncSharedState&, ExportFuncLocalState& localState,
    const ExportFuncBindData& bindData, std::vector<std::shared_ptr<ValueVector>> inputVectors);

void combineFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState);

void finalizeFunc(ExportFuncSharedState& sharedState);

} // namespace graphar_extension
} // namespace kuzu
