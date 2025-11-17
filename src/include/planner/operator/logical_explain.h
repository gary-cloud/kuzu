#pragma once

#include <utility>
#include <vector>

#include "common/enums/explain_type.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace planner {

class LogicalExplain final : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::EXPLAIN;

public:
    LogicalExplain(std::shared_ptr<LogicalOperator> child, common::ExplainType explainType,
        binder::expression_vector innerResultColumns, uint64_t topK,
        std::vector<LogicalPlan> candidatePlans)
        : LogicalOperator{type_, std::move(child)}, explainType{explainType},
          innerResultColumns{std::move(innerResultColumns)}, topK{topK},
          candidatePlans{std::move(candidatePlans)} {}

    void computeSchema();
    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override { return ""; }

    common::ExplainType getExplainType() const { return explainType; }

    binder::expression_vector getInnerResultColumns() const { return innerResultColumns; }
    uint64_t getTopK() const { return topK; }
    const std::vector<LogicalPlan>& getCandidatePlans() const { return candidatePlans; }
    bool hasCandidatePlans() const { return !candidatePlans.empty(); }

    std::unique_ptr<LogicalOperator> copy() override {
        std::vector<LogicalPlan> copiedPlans;
        copiedPlans.reserve(candidatePlans.size());
        for (auto& plan : candidatePlans) {
            copiedPlans.push_back(plan.copy());
        }
        return std::make_unique<LogicalExplain>(
            children[0], explainType, innerResultColumns, topK, std::move(copiedPlans));
    }

private:
    common::ExplainType explainType;
    binder::expression_vector innerResultColumns;
    uint64_t topK;
    std::vector<LogicalPlan> candidatePlans;
};

} // namespace planner
} // namespace kuzu
