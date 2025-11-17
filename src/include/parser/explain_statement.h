#pragma once

#include <memory>
#include <optional>

#include "common/enums/explain_type.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class ExplainStatement : public Statement {
public:
    ExplainStatement(std::unique_ptr<Statement> statementToExplain, common::ExplainType explainType,
        std::optional<uint64_t> topK)
        : Statement{common::StatementType::EXPLAIN},
          statementToExplain{std::move(statementToExplain)}, explainType{explainType},
          topK{topK} {}

    inline Statement* getStatementToExplain() const { return statementToExplain.get(); }

    inline common::ExplainType getExplainType() const { return explainType; }
    inline std::optional<uint64_t> getTopK() const { return topK; }

private:
    std::unique_ptr<Statement> statementToExplain;
    common::ExplainType explainType;
    std::optional<uint64_t> topK;
};

} // namespace parser
} // namespace kuzu
