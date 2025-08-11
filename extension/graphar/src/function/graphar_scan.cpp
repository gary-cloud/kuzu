#include "function/graphar_scan.h"

namespace kuzu {
namespace graphar_extension {

using namespace function;
using namespace common;

GrapharScanSharedState::GrapharScanSharedState(
    graphar::Result<std::shared_ptr<graphar::VerticesCollection>> maybeVerticesCollection,
    graphar::VertexIter vertex_iter)
    : maybe_vertices_collection{std::move(maybeVerticesCollection)},
      vertex_iter{std::move(vertex_iter)} {
        vertices_count = maybe_vertices_collection.value()->size();
}

std::unique_ptr<TableFuncSharedState> iniGrapharScanSharedState(
    const TableFuncInitSharedStateInput& input) {
    auto grapharScanBindData = input.bindData->constPtrCast<GrapharScanBindData>();
    auto maybe_vertices_collection = 
        graphar::VerticesCollection::Make(grapharScanBindData->graph_info, grapharScanBindData->table_name);
    auto vertex_iter = maybe_vertices_collection.value()->begin();
    return std::make_unique<graphar_extension::GrapharScanSharedState>(std::move(maybe_vertices_collection), std::move(vertex_iter));
}

offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto grapharSharedState = input.sharedState->ptrCast<graphar_extension::GrapharScanSharedState>();
    auto grapharScanBindData = input.bindData->constPtrCast<graphar_extension::GrapharScanBindData>();

    auto& column_names = grapharScanBindData->column_names;
    auto& column_setters = grapharScanBindData->column_setters;
    auto vertices = grapharSharedState->maybe_vertices_collection.value();
    size_t vertices_count = grapharSharedState->vertices_count;

    uint64_t count = 0;
    // iterate through vertices collection
    for (auto &it = grapharSharedState->vertex_iter; it != vertices->end(); ++it) {
        if (count >= 100) {
            break;
        }

        // int64_t id = it.property<int64_t>("id").value();
        // std::string firstName = it.property<std::string>("firstName").value();
        // std::string lastName = it.property<std::string>("lastName").value();
        // std::string gender = it.property<std::string>("gender").value();

        // output.dataChunk.getValueVectorMutable(grapharScanBindData->getFieldIdx("id"))
        //                 .setValue(count, id);
        // output.dataChunk.getValueVectorMutable(grapharScanBindData->getFieldIdx("firstName"))
        //                 .setValue(count, firstName);
        // output.dataChunk.getValueVectorMutable(grapharScanBindData->getFieldIdx("lastName"))
        //                 .setValue(count, lastName);
        // output.dataChunk.getValueVectorMutable(grapharScanBindData->getFieldIdx("gender"))
        //                 .setValue(count, gender);

        // TODO
        // for (const auto& column_name : column_names) {
        //     output.dataChunk.getValueVectorMutable(grapharScanBindData->getFieldIdx(column_name))
        //                     .setValue(count, it.property<xxx>(column_name).value());
        // }

        // Complete all column writes using the setter generated in the bind stage (without switch)
        for (size_t ci = 0; ci < column_setters.size(); ++ci) {
            column_setters[ci](it, output, (idx_t)count);
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
    function->initSharedStateFunc = iniGrapharScanSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace graphar_extension
} // namespace kuzu
