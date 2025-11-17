#pragma once

#include "binder/bound_statement.h"
#include <optional>
#include "common/enums/explain_type.h"

namespace kuzu {
namespace binder {

class BoundExplain final : public BoundStatement {
    static constexpr common::StatementType type_ = common::StatementType::EXPLAIN;

public:
    explicit BoundExplain(std::unique_ptr<BoundStatement> statementToExplain,
        common::ExplainType explainType, std::optional<uint64_t> topK)
        : BoundStatement{type_, BoundStatementResult::createSingleStringColumnResult(
                                    "explain result" /* columnName */)},
          statementToExplain{std::move(statementToExplain)}, explainType{explainType},
          topK{topK} {}

    BoundStatement* getStatementToExplain() const { return statementToExplain.get(); }

    common::ExplainType getExplainType() const { return explainType; }
    std::optional<uint64_t> getTopK() const { return topK; }

private:
    std::unique_ptr<BoundStatement> statementToExplain;
    common::ExplainType explainType;
    std::optional<uint64_t> topK;
};

} // namespace binder
} // namespace kuzu
