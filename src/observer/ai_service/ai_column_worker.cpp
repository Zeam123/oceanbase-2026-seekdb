#define USING_LOG_PREFIX SERVER
#include "observer/ai_service/ai_column_worker.h"

#include <charconv>
#include "common/json_type/ob_json_base.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/utility/utility.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_trigger_info.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "sql/resolver/dml/ob_update_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace observer
{
namespace {

enum AIJobState { PENDING, RUNNING, SUCCEEDED, RETRY_WAIT, FAILED };
constexpr int64_t MAX_AI_RESULT_BYTES = 4 * 1024 * 1024;

struct AIJob
{
  uint64_t column_id_ = 0;
  int64_t version_ = 0;
  int64_t state_ = PENDING;
  int64_t retries_ = 0;
  ObString key_;
  ObString lease_;
  ObString output_;
};

int append_ai_job_group(ObSqlString &sql, uint64_t table_id, const ObString &request)
{
  int ret = OB_SUCCESS;
  OZ (sql.append_fmt(" WHERE table_id=%lu AND request_id=", table_id));
  OZ (sql_append_hex_escape_str(request, sql));
  return ret;
}

int append_ai_job_identifier(ObSqlString &sql, const ObString &name)
{
  int ret = OB_SUCCESS;
  OZ (sql.append("`"));
  for (int64_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    if (name[i] == '`') { OZ (sql.append("`")); }
    OZ (sql.append(name.ptr() + i, 1));
  }
  OZ (sql.append("`"));
  return ret;
}

int select_ai_job_group(ObISQLClient &client, ObIAllocator &allocator, const char *query,
                        uint64_t &table_id, ObString &request)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObISQLClient::ReadResult, result) {
    if (OB_FAIL(client.read(result, query))) {
    } else if (OB_FAIL(result.get_result()->next())) {
    } else {
      ObString key;
      int64_t stored_id = 0;
      OZ (result.get_result()->get_int("table_id", stored_id));
      OZ (result.get_result()->get_varchar("request_id", key));
      if (OB_SUCC(ret) && (stored_id <= 0 || key.empty())) {
        ret = OB_INVALID_DATA;
      }
      OZ (ob_write_string(allocator, key, request));
      if (OB_SUCC(ret)) {
        table_id = static_cast<uint64_t>(stored_id);
      }
    }
  }
  return ret;
}

int claim_ai_job_group(ObISQLClient &proxy, ObIAllocator &allocator, uint64_t &table_id,
                       ObString &request, const ObString &lease)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction tx;
  const char *stage = "start";
  if (OB_FAIL(tx.start(&proxy))) {
  } else {
    stage = "select eligible";
    const char *eligible = "SELECT table_id,request_id FROM oceanbase.__all_ai_column_task "
                           "WHERE (state IN (0,3) AND eligible_ts<=NOW(6)) OR "
                           "(state=1 AND lease_expiry_ts<=NOW(6)) "
                           "ORDER BY eligible_ts,table_id,request_id LIMIT 1";
    ret = select_ai_job_group(tx, allocator, eligible, table_id, request);
    if (ret == OB_ITER_END) {
      // A restart can leave completed jobs waiting for their atomic publication.
      stage = "select completed";
      const char *completed = "SELECT table_id,request_id FROM oceanbase.__all_ai_column_task "
                              "GROUP BY table_id,request_id HAVING COUNT(*)=SUM(CASE "
                              "WHEN state IN (2,4) THEN 1 ELSE 0 END) "
                              "ORDER BY table_id,request_id LIMIT 1";
      ret = select_ai_job_group(tx, allocator, completed, table_id, request);
    }
    if (OB_SUCC(ret)) {
      stage = "lease update";
      ObSqlString sql;
      int64_t affected = 0;
      OZ (sql.append("UPDATE oceanbase.__all_ai_column_task SET state=1,lease_token="));
      OZ (sql_append_hex_escape_str(lease, sql));
      OZ (sql.append(",lease_expiry_ts=TIMESTAMPADD(SECOND,120,NOW(6))"));
      OZ (append_ai_job_group(sql, table_id, request));
      OZ (sql.append(" AND ((state IN (0,3) AND eligible_ts<=NOW(6)) OR "
                     "(state=1 AND lease_expiry_ts<=NOW(6)))"));
      OZ (tx.write(sql.ptr(), affected));
      if (OB_SUCC(ret) && affected == 0 && REACH_TIME_INTERVAL(1000 * 1000)) {
        LOG_INFO("AI column worker selected group without acquiring leases", K(table_id));
      }
    }
  }
  const int tx_ret = tx.handle_trans_in_the_end(ret == OB_ITER_END ? OB_SUCCESS : ret);
  if (tx_ret != OB_SUCCESS && REACH_TIME_INTERVAL(1000 * 1000)) {
    LOG_INFO("AI column worker claim failed", K(stage), K(ret), K(tx_ret), K(table_id));
  }
  return tx_ret == OB_SUCCESS ? ret : tx_ret;
}

int read_ai_job_group(ObISQLClient &client, ObIAllocator &allocator, const uint64_t table_id,
                      const ObString &request, const bool locking, ObIArray<AIJob> &jobs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  jobs.reuse();
  OZ (sql.append("SELECT column_id,definition_version,rowkey_payload,state,retry_count,"
                 "CAST(COALESCE(result_payload,'') AS CHAR) AS output,COALESCE(lease_token,'') AS lease "
                 "FROM oceanbase.__all_ai_column_task"));
  OZ (append_ai_job_group(sql, table_id, request));
  OZ (sql.append(locking ? " ORDER BY column_id FOR UPDATE" : " ORDER BY column_id"));
  SMART_VAR(ObISQLClient::ReadResult, result) {
    OZ (client.read(result, sql.ptr()));
    if (OB_SUCC(ret)) {
      auto &rows = *result.get_result();
      while (OB_SUCC(ret) && OB_SUCC(ret = rows.next())) {
        AIJob job;
        ObString key;
        ObString output;
        ObString lease;
        int64_t stored_id = 0;
        OZ (rows.get_int("column_id", stored_id));
        if (OB_SUCC(ret) && stored_id <= 0) { ret = OB_INVALID_DATA; }
        job.column_id_ = static_cast<uint64_t>(stored_id);
        OZ (rows.get_int("definition_version", job.version_));
        OZ (rows.get_int("state", job.state_));
        OZ (rows.get_int("retry_count", job.retries_));
        OZ (rows.get_varchar("rowkey_payload", key));
        OZ (rows.get_varchar("output", output));
        OZ (rows.get_varchar("lease", lease));
        OZ (ob_write_string(allocator, key, job.key_));
        OZ (ob_write_string(allocator, output, job.output_));
        OZ (ob_write_string(allocator, lease, job.lease_));
        OZ (jobs.push_back(job));
      }
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
  }
  return ret;
}

int build_ai_job_target(const ObTableSchema &table, const ObDatabaseSchema &database,
                        const ObString &payload, ObIAllocator &allocator,
                        ObSqlString &target, ObSqlString &where)
{
  int ret = OB_SUCCESS;
  ObRowkey rowkey;
  int64_t pos = 0;
  OZ (rowkey.deserialize(allocator, payload.ptr(), payload.length(), pos));
  if (OB_SUCC(ret) && (pos != payload.length() || rowkey.get_obj_cnt() != table.get_rowkey_column_num())) {
    ret = OB_INVALID_DATA;
  }
  OZ (append_ai_job_identifier(target, database.get_database_name_str()));
  OZ (target.append("."));
  OZ (append_ai_job_identifier(target, table.get_table_name_str()));
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
    uint64_t id = 0;
    const ObColumnSchemaV2 *column = nullptr;
    if (OB_FAIL(table.get_rowkey_info().get_column_id(i, id))) {
    } else if (OB_ISNULL(column = table.get_column_schema(id))) {
      ret = OB_SCHEMA_EAGAIN;
    } else {
      const ObObj &key = rowkey.get_obj_ptr()[i];
      OZ (where.append(i == 0 ? " WHERE " : " AND "));
      OZ (append_ai_job_identifier(where, column->get_column_name_str()));
      OZ (where.append("="));
      if (ob_is_int_tc(key.get_type())) {
        OZ (where.append_fmt("%ld", key.get_int()));
      } else if (key.is_integer_type()) {
        OZ (where.append_fmt("%lu", key.get_uint64()));
      } else if (key.is_varchar_or_char()) {
        if (!key.is_binary()) { OZ (where.append("CONVERT(")); }
        OZ (sql_append_hex_escape_str(key.get_string(), where));
        if (!key.is_binary()) {
          OZ (where.append_fmt(" USING %s)", ObCharset::charset_name(key.get_collation_type())));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
      }
    }
  }
  return ret;
}

const ObColumnSchemaV2 *get_ai_job_column(const ObTableSchema &table, const AIJob &job)
{
  const ObColumnSchemaV2 *column = table.get_column_schema(job.column_id_);
  return column != nullptr && column->is_ai_column() && column->get_schema_version() == job.version_ &&
         column->get_ai_column_definition().expression_.prefix_match("/* seekdb_ai_column_v1 */ ")
         ? column : nullptr;
}

int validate_ai_job_output(const ObColumnSchemaV2 &column, ObIAllocator &allocator, const ObString &text)
{
  int ret = OB_SUCCESS;
  if (text.length() > MAX_AI_RESULT_BYTES) {
    ret = OB_SIZE_OVERFLOW;
  } else if (ob_is_json_tc(column.get_data_type()) || ob_is_collection_sql_type(column.get_data_type())) {
    ObIJsonBase *value = nullptr;
    OZ (ObJsonBaseFactory::get_json_base(&allocator, text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, value));
  } else if (ob_is_integer_type(column.get_data_type())) {
    ObObj value;
    const char *begin = text.ptr();
    if (text.empty()) {
      ret = OB_INVALID_DATA;
    } else if (ob_is_int_tc(column.get_data_type())) {
      int64_t number = 0;
      const auto parsed = std::from_chars(begin, begin + text.length(), number);
      value.set_int(number);
      if (parsed.ec != std::errc{} || parsed.ptr != begin + text.length()) {
        ret = OB_INVALID_DATA;
      }
    } else {
      uint64_t number = 0;
      const auto parsed = std::from_chars(begin, begin + text.length(), number);
      value.set_uint64(number);
      if (parsed.ec != std::errc{} || parsed.ptr != begin + text.length()) {
        ret = OB_INVALID_DATA;
      }
    }
    if (OB_SUCC(ret) && value.is_overflow_integer(column.get_data_type())) {
      ret = OB_DATA_OUT_OF_RANGE;
    }
  }
  return ret;
}

int call_ai_job(ObISQLClient &proxy, ObIAllocator &allocator, const ObColumnSchemaV2 &column,
                const ObSqlString &target, const ObSqlString &where, ObString &output)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  OZ (sql.append("SELECT CAST(("));
  OZ (sql.append(column.get_ai_column_definition().expression_));
  OZ (sql.append(") AS CHAR) AS output FROM "));
  OZ (sql.append(target.string()));
  OZ (sql.append(where.string()));
  SMART_VAR(ObISQLClient::ReadResult, result) {
    OZ (proxy.read(result, sql.ptr()));
    if (OB_SUCC(ret)) {
      ObString value;
      OZ (result.get_result()->next());
      OZ (result.get_result()->get_varchar("output", value));
      OZ (validate_ai_job_output(column, allocator, value));
      OZ (ob_write_string(allocator, value, output));
    }
  }
  return ret;
}

int save_ai_job_outcome(ObISQLClient &proxy, uint64_t table_id, const ObString &request,
                        const AIJob &job, const int call_ret, const int64_t retry_limit,
                        const ObString &output)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const bool retry = call_ret != OB_SUCCESS && call_ret != OB_ITER_END && job.retries_ < retry_limit;
  const int state = call_ret == OB_SUCCESS ? SUCCEEDED : (retry ? RETRY_WAIT : FAILED);
  OZ (sql.append_fmt("UPDATE oceanbase.__all_ai_column_task SET state=%d,lease_token=NULL,"
                     "lease_expiry_ts=NULL,last_error=%d,result_payload=", state, call_ret));
  if (state == SUCCEEDED) {
    OZ (sql_append_hex_escape_str(output, sql));
  } else {
    OZ (sql.append("NULL"));
  }
  if (retry) {
    OZ (sql.append(",retry_count=retry_count+1,eligible_ts=TIMESTAMPADD(SECOND,1,NOW(6))"));
  }
  OZ (append_ai_job_group(sql, table_id, request));
  OZ (sql.append_fmt(" AND column_id=%lu AND state=1 AND lease_token=", job.column_id_));
  OZ (sql_append_hex_escape_str(job.lease_, sql));
  int64_t affected = 0;
  OZ (proxy.write(sql.ptr(), affected));
  return ret;
}

int publish_ai_job_group(ObISQLClient &proxy, ObIAllocator &allocator, const ObTableSchema &table,
                         const ObString &request, const ObString &key,
                         const ObSqlString &target, const ObSqlString &where)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = table.get_table_id();
  ObMySQLTransaction tx;
  ObSEArray<AIJob, 4> jobs;
  bool row_exists = false;
  bool finished = true;
  if (OB_FAIL(tx.start(&proxy))) {
  } else {
    // Business DML locks the row before changing the queue. Use the same order.
    ObSqlString lock_sql;
    OZ (lock_sql.append("SELECT 1 FROM "));
    OZ (lock_sql.append(target.string()));
    OZ (lock_sql.append(where.string()));
    OZ (lock_sql.append(" FOR UPDATE"));
    {
      // SMART_VAR retains the result until the enclosing scope exits.
      SMART_VAR(ObISQLClient::ReadResult, result) {
        OZ (tx.read(result, lock_sql.ptr()));
        if (OB_SUCC(ret)) {
          ret = result.get_result()->next();
          row_exists = ret == OB_SUCCESS;
          if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
        }
      }
    }
    OZ (read_ai_job_group(tx, allocator, table_id, request, true, jobs));
    for (int64_t i = 0; OB_SUCC(ret) && finished && i < jobs.count(); ++i) {
      const AIJob &job = jobs.at(i);
      if (job.key_ != key) {
        ret = OB_INVALID_DATA;
      } else {
        finished = job.state_ == SUCCEEDED || job.state_ == FAILED;
      }
    }
  }
  if (OB_SUCC(ret) && !jobs.empty() && (!row_exists || finished)) {
    int64_t affected = 0;
    if (row_exists) {
      ObSqlString update;
      bool has_assignment = false;
      OZ (update.append("UPDATE /*+ USE_PLAN_CACHE(NONE) */ "));
      OZ (update.append(target.string()));
      OZ (update.append(" SET "));
      for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
        const AIJob &job = jobs.at(i);
        const ObColumnSchemaV2 *column = get_ai_job_column(table, job);
        if (column != nullptr) {
          const uint64_t state_id = column->get_ai_column_definition().state_column_id_;
          const ObColumnSchemaV2 *state_column = table.get_column_schema(state_id);
          if (OB_ISNULL(state_column)) {
            ret = OB_SCHEMA_EAGAIN;
          } else {
            if (has_assignment) { OZ (update.append(",")); }
            OZ (append_ai_job_identifier(update, column->get_column_name_str()));
            if (job.state_ == SUCCEEDED) {
              OZ (update.append("=CONVERT("));
              OZ (sql_append_hex_escape_str(job.output_, update));
              OZ (update.append(" USING utf8mb4),"));
            } else {
              OZ (update.append("=NULL,"));
            }
            OZ (append_ai_job_identifier(update, state_column->get_column_name_str()));
            OZ (update.append(job.state_ == SUCCEEDED ? "=1" : "=2"));
            has_assignment = true;
          }
        }
      }
      if (OB_SUCC(ret) && has_assignment) {
        OZ (update.append(where.string()));
        sql::AIColumnPublishGuard guard(table_id);
        OZ (tx.write(update.ptr(), affected));
      }
    }
    ObSqlString clear;
    OZ (clear.append("DELETE FROM oceanbase.__all_ai_column_task"));
    OZ (append_ai_job_group(clear, table_id, request));
    OZ (tx.write(clear.ptr(), affected));
  }
  return tx.handle_trans_in_the_end(ret);
}

int run_ai_job_group(ObISQLClient &proxy, ObMultiVersionSchemaService &schemas)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AIColumnWorker");
  char lease_bytes[sql::UuidCommon::LENGTH_UUID] = {};
  const ObString lease(sizeof(lease_bytes), lease_bytes);
  uint64_t table_id = OB_INVALID_ID;
  ObString request;
  ObSEArray<AIJob, 4> jobs;
  ObSchemaGetterGuard guard;
  const ObTableSchema *table = nullptr;
  const ObDatabaseSchema *database = nullptr;
  bool update_trigger = false;
  ObSqlString target;
  ObSqlString where;
  if (OB_FAIL(sql::ObExprUuid::gen_server_uuid(lease_bytes, sizeof(lease_bytes)))) {
  } else if (OB_FAIL(claim_ai_job_group(proxy, allocator, table_id, request, lease))) {
  } else if (OB_FAIL(read_ai_job_group(proxy, allocator, table_id, request, false, jobs))) {
  } else if (jobs.empty()) {
  } else if (OB_FAIL(schemas.get_runtime_schema_guard(guard))) {
  } else if (OB_FAIL(guard.get_table_schema(table_id, table))) {
  } else if (OB_ISNULL(table) || !table->has_ai_columns()) {
    ret = OB_SCHEMA_EAGAIN;
  } else if (OB_FAIL(table->check_has_trigger_on_table(guard, update_trigger,
                                                       ObTriggerEvents::get_update_event()))) {
  } else if (update_trigger) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), database))) {
  } else if (OB_ISNULL(database)) {
    ret = OB_SCHEMA_EAGAIN;
  } else if (OB_FAIL(build_ai_job_target(*table, *database, jobs.at(0).key_, allocator, target, where))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < jobs.count(); ++i) {
      const AIJob &job = jobs.at(i);
      if (job.state_ == RUNNING && job.lease_ == lease) {
        const ObColumnSchemaV2 *column = get_ai_job_column(*table, job);
        ObString output;
        const int call_ret = column == nullptr ? OB_SCHEMA_EAGAIN :
                             call_ai_job(proxy, allocator, *column, target, where, output);
        if (call_ret != OB_SUCCESS && call_ret != OB_ITER_END) {
          LOG_INFO("AI column worker model call failed", K(table_id), K(job.column_id_), K(job.retries_), K(call_ret));
        }
        const int64_t retry_limit = column == nullptr ? 0 : column->get_ai_column_definition().max_retries_;
        OZ (save_ai_job_outcome(proxy, table_id, request, job, call_ret, retry_limit, output));
      }
    }
    OZ (publish_ai_job_group(proxy, allocator, *table, request, jobs.at(0).key_, target, where));
  }
  return ret;
}

} // namespace

void AIColumnWorker::runTimerTask()
{
  if (GCTX.server_role_ == share::ObServerRole::PRIMARY_ROLE &&
      GCTX.sql_proxy_ != nullptr && GCTX.schema_service_ != nullptr) {
    const int ret = run_ai_job_group(*GCTX.sql_proxy_, *GCTX.schema_service_);
    if (ret != OB_SUCCESS && ret != OB_ITER_END && REACH_TIME_INTERVAL(1000 * 1000)) {
      LOG_INFO("AI column worker failed to process group", K(ret));
    } else if (ret == OB_ITER_END && REACH_TIME_INTERVAL(5000 * 1000)) {
      LOG_INFO("AI column worker found no eligible group");
    }
  } else if (REACH_TIME_INTERVAL(5000 * 1000)) {
    LOG_INFO("AI column worker paused", "role", GCTX.server_role_,
             KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  }
}

} // namespace observer
} // namespace oceanbase
