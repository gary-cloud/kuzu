#include "function/graphar_scan.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

GrapharScanSharedState::GrapharScanSharedState(
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybeVerticesCollection, uint64_t max_threads)
    : maybe_vertices_collection{std::move(maybeVerticesCollection)} {
        vertices_count = maybe_vertices_collection.value()->size();
        batch_size = max_threads % vertices_count == 0 ? vertices_count / max_threads : vertices_count / max_threads + 1;
        next_index.store(0);
}

std::unique_ptr<TableFuncSharedState> initGrapharScanSharedState(
    const TableFuncInitSharedStateInput& input) {
    auto grapharScanBindData = input.bindData->constPtrCast<GrapharScanBindData>();
    auto maybe_vertices_collection = 
        graphar::VerticesCollection::Make(grapharScanBindData->graph_info, grapharScanBindData->table_name);
    return std::make_unique<graphar_extension::GrapharScanSharedState>(std::move(maybe_vertices_collection), grapharScanBindData->max_threads);
}

offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto grapharSharedState = input.sharedState->ptrCast<graphar_extension::GrapharScanSharedState>();
    auto grapharScanBindData = input.bindData->constPtrCast<graphar_extension::GrapharScanBindData>();

    auto& column_setters = grapharScanBindData->column_setters;
    auto vertices = grapharSharedState->maybe_vertices_collection.value();
    size_t vertices_count = grapharSharedState->vertices_count;
    size_t batch_size = grapharSharedState->batch_size;

    // Reserve a batch of indices atomically
    size_t start = grapharSharedState->next_index.fetch_add(batch_size, std::memory_order_relaxed);
    if (start >= vertices_count) {
        // no more rows
        output.dataChunk.state->getSelVectorUnsafe().setSelSize(0);
        return 0;
    }
    size_t end = std::min(start + batch_size, vertices_count);

    idx_t count = 0;
    for (size_t idx = start; idx < end; ++idx) {
        // create a local iterator for this index (each worker/thread has its own local iterator)
        auto it = vertices->begin() + idx;

        // Complete all column writes using the setter generated in the bind stage (without switch)
        for (size_t ci = 0; ci < column_setters.size(); ++ci) {
            column_setters[ci](it, output, count);
        }

        count++;
    }
    output.dataChunk.state->getSelVectorUnsafe().setSelSize(count);
    return output.dataChunk.state->getSelVector().getSelSize();
}

function_set GrapharScanFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initGrapharScanSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace graphar_extension
} // namespace kuzu
