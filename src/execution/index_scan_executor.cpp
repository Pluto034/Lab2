//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  if(inited) return;
  auto index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
  htable_->ScanKey(Tuple{ std::vector<Value>{plan_->pred_key_->val_}, htable_->GetKeySchema() }, &rids_, exec_ctx_->GetTransaction());
  iterator_ = rids_.begin();
  inited = true;
  // throw NotImplementedException("IndexScanExecutor is not implemented");
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iterator_ != rids_.end()) {
    const auto tmp_rid = *iterator_;
    const auto [tuple_mata, tuple_val] = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(tmp_rid);
    // std::cerr<< tuple_val.ToString(&exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->schema_)<<std::endl;
    if(tuple_mata.is_deleted_) {
      ++iterator_;
      continue;
    }
    *rid = tmp_rid;
    *tuple = tuple_val;
    ++iterator_;
    return true;
  }

  return false;
}

}  // namespace bustub
