//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  if (iterator_ != nullptr) return;

  const auto lpCatalog = exec_ctx_->GetCatalog();
  const auto lptable = lpCatalog->GetTable(plan_->table_oid_);
  const auto &tableRef = lptable->table_;

  iterator_ = std::make_unique<TableIterator>(tableRef->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_->IsEnd()) {
    *tuple = Tuple::Empty();
    return false;
  }
  for (; not iterator_->IsEnd(); ++*iterator_) {
    auto item = iterator_->GetTuple();
    if (not item.first.is_deleted_) {
      if (plan_->filter_predicate_.get() != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(&item.second, GetOutputSchema());
        if (!value.IsNull() and value.GetAs<bool>()) {
          break;
        }
      } else {
        break;
      }
    }
  }
  if (iterator_->IsEnd()) {
    *tuple = Tuple::Empty();
    return false;
  }

  *tuple = iterator_->GetTuple().second;
  *rid = iterator_->GetRID();
  ++(*iterator_);
  return true;
}

}  // namespace bustub
