#include <execution/expressions/column_value_expression.h>
#include <execution/expressions/comparison_expression.h>
#include <execution/plans/index_scan_plan.h>
#include <execution/plans/seq_scan_plan.h>

#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  auto seq_plan = dynamic_cast<const SeqScanPlanNode*>(plan.get());
  if(seq_plan == nullptr) return plan;

  auto filter_predicate = dynamic_cast<ComparisonExpression*> (seq_plan->filter_predicate_.get());
  if(filter_predicate == nullptr) return plan;
  if(filter_predicate->comp_type_ != ComparisonType::Equal) return plan;

  auto indexes = catalog_.GetTableIndexes(seq_plan->table_name_);

  BUSTUB_ENSURE(filter_predicate->children_.size() == 2, "For comparison nodes, the number of child nodes must be 2.");

  ConstantValueExpression* val = nullptr;
  for(const auto& node: filter_predicate->children_) {
    auto col_node = dynamic_cast<ConstantValueExpression*>(node.get());
    if(col_node != nullptr) {
      val = col_node;
      break;
    }
  }
  if(val == nullptr) return plan;

  for(const auto& node: filter_predicate->children_) {
    auto col_node = dynamic_cast<ColumnValueExpression*>(node.get());
    if(col_node != nullptr) {
      IndexInfo* index_info_ = nullptr;
      for(auto idx: indexes) {
        if(idx->index_->GetMetadata()->GetKeyAttrs().at(0) == col_node->GetColIdx()) {
          index_info_ = idx;
        }
      }
      if(index_info_ == nullptr) return plan;
      auto res = std::make_shared<const IndexScanPlanNode>(seq_plan->output_schema_,
        seq_plan->table_oid_,
        index_info_->index_oid_,
        seq_plan->filter_predicate_,
        val
        );
      return res;
    }
  }

  return plan;
}

}  // namespace bustub
