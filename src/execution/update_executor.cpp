//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  // throw NotImplementedException("UpdateExecutor is not implemented");
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(child_executor_ == nullptr) return false;
  RID tmp_rid{};
  int total_mdf = 0;
  Tuple tmp_tuple{};

  while(child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    auto table = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
    auto& schema = table->schema_;

    auto len = schema.GetColumnCount();
    std::vector<Value> new_values { };
    for(decltype(len) i = 0;i<len;i++) {
      auto new_val = plan_->target_expressions_[i]->Evaluate(&tmp_tuple, schema);
      new_values.emplace_back(std::move(new_val));
    }
    Tuple new_tuple = Tuple{std::move(new_values), &schema};

    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);

    // 删除指定记录
    table->table_->UpdateTupleMeta(TupleMeta{0, true}, tmp_rid);
    // 删除对应索引
    for(auto index_info_: indexes) {
      auto htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
      htable_->DeleteEntry(tmp_tuple.KeyFromTuple(table->schema_, *htable_->GetKeySchema(), htable_->GetKeyAttrs()), tmp_rid, exec_ctx_->GetTransaction());
    }

    // 插入新记录
    auto new_rid = table->table_->InsertTuple(TupleMeta{0, false}, new_tuple);
    BUSTUB_ENSURE(new_rid.has_value(), "Update fail.");
    // 插入对应索引
    for(auto index_info_: indexes) {
      auto htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
      htable_->InsertEntry(new_tuple.KeyFromTuple(table->schema_, *htable_->GetKeySchema(), htable_->GetKeyAttrs()), new_rid.value(), exec_ctx_->GetTransaction());
    }

    total_mdf ++;
  }

  child_executor_.reset(nullptr);
  std::vector<Value> new_values { Value{TypeId::INTEGER, total_mdf} };
  *tuple = Tuple{new_values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
