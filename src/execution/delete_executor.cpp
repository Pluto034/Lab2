//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  // throw NotImplementedException("DeleteExecutor is not implemented");
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (child_executor_ == nullptr) return false;
  RID tmp_rid{};
  int total_del = 0;
  Tuple tmp_tuple{};

  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    auto table = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);

    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);

    // 删除指定记录
    table->table_->UpdateTupleMeta(TupleMeta{0, true}, tmp_rid);
    // 删除对应索引
    for (auto index_info_ : indexes) {
      auto htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
      htable_->DeleteEntry(tmp_tuple.KeyFromTuple(table->schema_, *htable_->GetKeySchema(), htable_->GetKeyAttrs()),
                           tmp_tuple.GetRid(), exec_ctx_->GetTransaction());
    }

    total_del++;
  }

  child_executor_.reset(nullptr);
  std::vector<Value> new_values{Value{TypeId::INTEGER, total_del}};
  *tuple = Tuple{new_values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
