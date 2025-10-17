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
    const ExportFuncBindData& bindData, std::vector<bool>) {
    const ExportGrapharBindData& grapharBindData =
        bindData.constCast<ExportGrapharBindData>();
    return std::make_unique<ExportGrapharLocalState>(grapharBindData.schema);
}

void sinkFunc(ExportFuncSharedState&, ExportFuncLocalState& localState,
    [[maybe_unused]] const ExportFuncBindData& bindData, std::vector<std::shared_ptr<ValueVector>> inputVectors) {
    auto& grapharLocalState = localState.cast<ExportGrapharLocalState>();

    auto& buffer = grapharLocalState.buffer;
    auto& schema = grapharLocalState.buffer.Schema();

    // fill the buffer with input vectors.
    for (size_t i = 0; i < inputVectors.size(); i++) {
        size_t rid = buffer.NewRow();

        for (size_t j = 0; j < schema.size(); j++) {
            switch (schema[j].type) {
            case Type::INT64:
            case Type::TIMESTAMP:
                buffer.SetProperty(rid, schema[j].name,
                    Scalar(inputVectors[i]->getValue<int64_t>(j)));
                break;
            case Type::INT32:
            case Type::DATE:
                buffer.SetProperty(rid, schema[j].name,
                    Scalar(inputVectors[i]->getValue<int32_t>(j)));
                break;
            case Type::DOUBLE:
                buffer.SetProperty(rid, schema[j].name,
                    Scalar(inputVectors[i]->getValue<double>(j)));
                break;
            case Type::FLOAT:
                buffer.SetProperty(rid, schema[j].name,
                    Scalar(inputVectors[i]->getValue<float>(j)));
                break;
            case Type::STRING:
                buffer.SetProperty(rid, schema[j].name,
                    Scalar(inputVectors[i]->getValue<std::string>(j)));
                break;
            case Type::BOOL:
                buffer.SetProperty(rid, schema[j].name, Scalar(inputVectors[i]->getValue<bool>(j)));
                break;
            default:
                throw common::RuntimeException(
                    common::stringFormat("Unsupported type for property '{}'", schema[j].name));
            }
        }
    }
}

void combineFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState) {
    auto& grapharSharedState = sharedState.cast<ExportGrapharSharedState>();
    auto& grapharLocalState = localState.cast<ExportGrapharLocalState>();

    auto& buffer = grapharLocalState.buffer;
    auto& schema = grapharLocalState.buffer.Schema();

    // Helper: find index of a property by a list of candidate names (case-sensitive).
    auto findIndexByCandidates =
        [&schema](const std::vector<std::string>& candidates) -> std::optional<size_t> {
        for (const auto& cand : candidates) {
            for (size_t i = 0; i < schema.size(); ++i) {
                if (schema[i].name == cand)
                    return i;
            }
        }
        return std::nullopt;
    };

    // Candidate names for edge endpoints
    const std::vector<std::string> srcCandidates = {"src", "source", "from", "src_id", "srcId",
        "source_id"};
    const std::vector<std::string> dstCandidates = {"dst", "dest", "target", "to", "dst_id",
        "dstId", "target_id"};

    // If exporting edges, try to locate src/dst indices once before looping.
    std::optional<size_t> srcIdxOpt, dstIdxOpt;
    if (grapharSharedState.is_edge) {
        srcIdxOpt = findIndexByCandidates(srcCandidates);
        dstIdxOpt = findIndexByCandidates(dstCandidates);
        if (!srcIdxOpt || !dstIdxOpt) {
            // If not found, try a fallback: assume first two columns are src/dst
            if (schema.size() >= 2) {
                srcIdxOpt = 0;
                dstIdxOpt = 1;
            } else {
                throw common::RuntimeException{common::stringFormat(
                    "Cannot locate src/dst properties in schema for edge export.")};
            }
        }
    }

    std::lock_guard<std::mutex> lck{grapharSharedState.mtx};

    // Iterate over rows and convert each row into a Vertex or Edge.
    for (auto& row : buffer.GetRows()) {
        if (!grapharSharedState.is_edge) {
            // Vertex export
            graphar::builder::Vertex vertex;

            // For each property in schema, if it's set, push to vertex.
            for (size_t i = 0; i < schema.size(); ++i) {
                const auto& meta = schema[i];
                const auto& cell = row.props[i];

                // Skip unset properties.
                if (std::holds_alternative<std::monostate>(cell))
                    continue;

                if (meta.card == Cardinality::SINGLE) {
                    // Single-valued property
                    const Scalar& s = std::get<Scalar>(cell);
                    std::visit(
                        [&](auto&& val) {
                            using T = std::decay_t<decltype(val)>;
                            if constexpr (std::is_same_v<T, int64_t>) {
                                vertex.AddProperty(meta.name, val);
                            } else if constexpr (std::is_same_v<T, int32_t>) {
                                vertex.AddProperty(meta.name, val);
                            } else if constexpr (std::is_same_v<T, double>) {
                                vertex.AddProperty(meta.name, val);
                            } else if constexpr (std::is_same_v<T, float>) {
                                vertex.AddProperty(meta.name, val);
                            } else if constexpr (std::is_same_v<T, std::string>) {
                                vertex.AddProperty(meta.name, val);
                            } else if constexpr (std::is_same_v<T, bool>) {
                                vertex.AddProperty(meta.name, val);
                            } else {
                                // unreachable
                            }
                        },
                        s);
                } else {
                    throw common::NotImplementedException{
                        "GraphAr edge multi-valued properties are not implemented."};
                }
            }

            // Add constructed vertex to verticesBuilder and check status.
            if (!grapharSharedState.verticesBuilder) {
                throw common::RuntimeException{"verticesBuilder is null."};
            }
            auto st_v = grapharSharedState.verticesBuilder->AddVertex(vertex);
            if (!st_v.ok()) {
                throw common::RuntimeException{common::stringFormat("AddVertex failed: {}", st_v.message())};
            }

        } else {
            // Edge export
            // retrieve src/dst indices (guaranteed present above)
            size_t srcIdx = *srcIdxOpt;
            size_t dstIdx = *dstIdxOpt;

            // src/dst must be set and scalar
            const auto& srcCell = row.props[srcIdx];
            const auto& dstCell = row.props[dstIdx];
            if (std::holds_alternative<std::monostate>(srcCell) ||
                std::holds_alternative<std::monostate>(dstCell)) {
                throw common::RuntimeException{"edge row missing src or dst value."};
            }
            if (!std::holds_alternative<Scalar>(srcCell) ||
                !std::holds_alternative<Scalar>(dstCell)) {
                throw common::RuntimeException{"edge src/dst must be scalar."};
            }

            // Extract src/dst as integer (prefer int64_t, allow int32_t)
            int64_t srcId = 0, dstId = 0;
            const Scalar& ssrc = std::get<Scalar>(srcCell);
            const Scalar& sdst = std::get<Scalar>(dstCell);
            // visitor for extracting integer id
            auto extractId = [](const Scalar& sc) -> int64_t {
                if (std::holds_alternative<int64_t>(sc))
                    return std::get<int64_t>(sc);
                if (std::holds_alternative<int32_t>(sc))
                    return static_cast<int64_t>(std::get<int32_t>(sc));
                // also allow strings that look like integers? Not by default: throw
                throw common::RuntimeException{"edge endpoint is not integer type."};
            };
            srcId = extractId(ssrc);
            dstId = extractId(sdst);

            // construct edge
            graphar::builder::Edge edge(srcId, dstId);

            // add remaining properties except src/dst themselves
            for (size_t i = 0; i < schema.size(); ++i) {
                if (i == srcIdx || i == dstIdx)
                    continue; // skip endpoints
                const auto& meta = schema[i];
                const auto& cell = row.props[i];
                if (std::holds_alternative<std::monostate>(cell))
                    continue;

                if (meta.card == Cardinality::SINGLE) {
                    const Scalar& s = std::get<Scalar>(cell);
                    std::visit(
                        [&](auto&& val) {
                            using T = std::decay_t<decltype(val)>;
                            if constexpr (std::is_same_v<T, int64_t>) {
                                edge.AddProperty(meta.name, val);
                            } else if constexpr (std::is_same_v<T, int32_t>) {
                                edge.AddProperty(meta.name, val);
                            } else if constexpr (std::is_same_v<T, double>) {
                                edge.AddProperty(meta.name, val);
                            } else if constexpr (std::is_same_v<T, float>) {
                                edge.AddProperty(meta.name, val);
                            } else if constexpr (std::is_same_v<T, std::string>) {
                                edge.AddProperty(meta.name, val);
                            } else if constexpr (std::is_same_v<T, bool>) {
                                edge.AddProperty(meta.name, val);
                            }
                        },
                        s);
                } else {
                    throw common::NotImplementedException{
                        "GraphAr edge multi-valued properties are not implemented."};
                }
            }

            // add edge to edgesBuilder and check status
            if (!grapharSharedState.edgesBuilder) {
                throw common::RuntimeException{"edgesBuilder is null."};
            }
            auto st_e = grapharSharedState.edgesBuilder->AddEdge(edge);
            if (!st_e.ok()) {
                throw common::RuntimeException{common::stringFormat("AddEdge failed: {}", st_e.message())};
            }
        }
    }
}

void finalizeFunc(ExportFuncSharedState& sharedState) {
    auto& grapharSharedState = sharedState.cast<ExportGrapharSharedState>();

    if (!grapharSharedState.is_edge) {
        if (!grapharSharedState.verticesBuilder) {
            throw common::RuntimeException{"verticesBuilder is null in finalize."};
        }
        auto st = grapharSharedState.verticesBuilder->Dump();
        if (!st.ok()) {
            throw common::RuntimeException{
                common::stringFormat("VerticesBuilder Finalize failed: {}", st.message())};
        }
        grapharSharedState.verticesBuilder->Clear();
    } else {
        if (!grapharSharedState.edgesBuilder) {
            throw common::RuntimeException{"edgesBuilder is null in finalize."};
        }
        auto st = grapharSharedState.edgesBuilder->Dump();
        if (!st.ok()) {
            throw common::RuntimeException{
                common::stringFormat("EdgesBuilder Finalize failed: {}", st.message())};
        }
        grapharSharedState.edgesBuilder->Clear();
    }
}

void ExportGrapharSharedState::init([[maybe_unused]] main::ClientContext& context,
    const ExportFuncBindData& bindData) {
    const ExportGrapharBindData& grapharBindData = bindData.constCast<ExportGrapharBindData>();
    std::shared_ptr<GraphInfo> graph_info = grapharBindData.graphInfo;
    GrapharExportOptions exportOptions = grapharBindData.exportOptions;
    std::string tableName = grapharBindData.tableName;
    ValidateLevel validateLevel = grapharBindData.validateLevel;

    auto vertex_infos = graph_info->GetVertexInfos();
    auto edge_infos = graph_info->GetEdgeInfos();

    // Try vertex first
    for (const auto& v_info : vertex_infos) {
        if (v_info->GetType() == tableName) {
            vertexInfo = v_info;
            verticesBuilder = std::make_shared<builder::VerticesBuilder>(vertexInfo, "/tmp/", 0L,
                exportOptions.wopt, validateLevel);
            is_edge = false;
            return;
        }
    }

    // Try edge if not vertex
    for (const auto& e_info : edge_infos) {
        std::string src_type = e_info->GetSrcType();
        std::string edge_type = e_info->GetEdgeType();
        std::string dst_type = e_info->GetDstType();
        std::string full_edge_name1 = src_type + "." + edge_type + "." + dst_type;
        std::string full_edge_name2 = src_type + ":" + edge_type + ":" + dst_type;
        std::string full_edge_name3 = src_type + "_" + edge_type + "_" + dst_type;
        if (full_edge_name1 == tableName || full_edge_name2 == tableName ||
            full_edge_name3 == tableName) {
            edgeInfo = e_info;
            edgesBuilder = std::make_shared<builder::EdgesBuilder>(edgeInfo, "/tmp/",
                AdjListType::ordered_by_source, 903, exportOptions.wopt, validateLevel);
            is_edge = true;
            return;
        }
    }
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
