//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), insert_item_(std::move(child_executor)) {}

/*
 * create table t1(v1 int, v2 varchar(128), v3 int);
 * insert into t1 values (0, '🥰', 10), (1, '🥰🥰', 11), (2, '🥰🥰🥰', 12), (3, '🥰🥰🥰🥰', 13), (5, '🥰🥰🥰🥰🥰', 14);
 */
void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(insert_item_ != nullptr) {
    RID tmp_rid{};
    Tuple tmp_tuple{};
    std::int32_t insert_cnt = 0;
    insert_item_->Init();
    for (;insert_item_->Next(&tmp_tuple, &tmp_rid); insert_cnt++) {
      // TODO timestamp
      auto meta = TupleMeta{0 , false};
      auto table = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
      auto res_rid = table->table_->InsertTuple(meta, tmp_tuple);
      BUSTUB_ENSURE(res_rid.has_value(), "Insert fail.");
      auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);

      for(auto index_info_: indexes) {
        auto htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
        htable_->InsertEntry(tmp_tuple.KeyFromTuple(table->schema_, *htable_->GetKeySchema(), htable_->GetKeyAttrs()), res_rid.value(), exec_ctx_->GetTransaction());
      }
    }
    insert_item_.reset(nullptr);

    std::vector<Value> values;
    values.emplace_back(TypeId::INTEGER, insert_cnt );
    *tuple = Tuple{values, &GetOutputSchema()};

    return true;
  }
  
  *tuple = Tuple::Empty();
  return false;
  // this->insert_item_->GetExecutorContext().;
  // exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->InsertTuple()
  //return false;
}

}  // namespace bustub
