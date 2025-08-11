#include "function/graphar_scan.h"

#include "common/exception/not_implemented.h"

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
    
    // throw NotImplementedException{"GrapharScanFunction::initGrapharScanSharedState is not implemented yet."};
}

offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto grapharSharedState = input.sharedState->ptrCast<graphar_extension::GrapharScanSharedState>();
    auto grapharScanBindData = input.bindData->constPtrCast<graphar_extension::GrapharScanBindData>();

    auto column_names = grapharScanBindData->column_names;
    auto vertices = grapharSharedState->maybe_vertices_collection.value();
    size_t vertices_count = grapharSharedState->vertices_count;

    size_t count = 0;
    // iterate through vertices collection
    for (auto &it = grapharSharedState->vertex_iter; it != vertices->end(); ++it) {
        if (count >= 100) {
            break;
        }

        auto id = it.property<int64_t>("id").value();
        auto firstName = it.property<std::string>("firstName").value();
        auto lastName = it.property<std::string>("lastName").value();
        auto gender = it.property<std::string>("gender").value();

        output.dataChunk.getValueVectorMutable(grapharScanBindData->getFieldIdx("id"))
                        .setValue(count, id);
        output.dataChunk.getValueVectorMutable(grapharScanBindData->getFieldIdx("firstName"))
                        .setValue(count, firstName);
        output.dataChunk.getValueVectorMutable(grapharScanBindData->getFieldIdx("lastName"))
                        .setValue(count, lastName);
        output.dataChunk.getValueVectorMutable(grapharScanBindData->getFieldIdx("gender"))
                        .setValue(count, gender);

        // TODO
        // for (const auto& column_name : column_names) {
        //     output.dataChunk.getValueVectorMutable(grapharScanBindData->getFieldIdx(column_name))
        //                     .setValue(count, it.property<std::string>(column_name).value());
        // }
        count++;
    }
    output.dataChunk.state->getSelVectorUnsafe().setSelSize(count);
    return output.dataChunk.state->getSelVector().getSelSize();

    // throw NotImplementedException{"GrapharScanFunction::tableFunc is not implemented yet."};
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

    // throw NotImplementedException{"GrapharScanFunction::getFunctionSet is not implemented yet."};
}

} // namespace graphar_extension
} // namespace kuzu
