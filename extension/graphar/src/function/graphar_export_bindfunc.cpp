#include "function/graphar_export.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

ExportGrapharBindData::ExportGrapharBindData(std::vector<std::string> columnNames,
    const std::string& fileName)
    : ExportFuncBindData(std::move(columnNames), fileName) {}

ExportGrapharBindData::ExportGrapharBindData(std::vector<std::string> columnNames,
    std::vector<LogicalType> columnTypes, std::string fileName)
    : ExportFuncBindData(std::move(columnNames), std::move(fileName)) {
    setDataType(std::move(columnTypes));
}

std::unique_ptr<ExportFuncBindData> bindFunc(ExportFuncBindInput& bindInput) {
    return std::make_unique<ExportGrapharBindData>(bindInput.columnNames, bindInput.filePath);
}

} // namespace graphar_extension
} // namespace kuzu