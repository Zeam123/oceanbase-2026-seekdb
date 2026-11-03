/*
 * Copyright (c) 2025 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */
#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_file_scan_utils.h"
#include "common/json_type/ob_json_common.h"
#include "common/json_type/ob_json_base.h"
#include "common/json_type/ob_json_tree.h"
#include "common/timezone/ob_time_convert.h"
#include "lib/string/ob_string.h"
#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <limits.h>
#include <fstream>
#include <map>
#include <string>
#include <vector>
#ifndef _WIN32
#include <glob.h>
#include <limits.h>
#include <unistd.h>
#else
#include <windows.h>
#endif

namespace oceanbase {
namespace sql {
using namespace common;
namespace {

enum InferredKind { IK_NULL, IK_BOOL, IK_INT, IK_DOUBLE, IK_DATE, IK_DATETIME, IK_VARCHAR };

bool read_csv_record(std::istream &input, std::vector<std::string> &fields, int64_t &record_no)
{
  fields.clear();
  std::string value;
  bool quoted = false;
  bool any = false;
  while (true) {
    int next = input.get();
    if (next == EOF) {
      if (!any && value.empty() && fields.empty()) { return false; }
      if (quoted) { return false; }
      fields.push_back(value);
      ++record_no;
      return true;
    }
    any = true;
    char ch = static_cast<char>(next);
    if (quoted) {
      if (ch == '"') {
        if (input.peek() == '"') { input.get(); value.push_back('"'); }
        else { quoted = false; }
      } else { value.push_back(ch); }
    } else if (ch == '"' && value.empty()) {
      quoted = true;
    } else if (ch == ',') {
      fields.push_back(value);
      value.clear();
    } else if (ch == '\n') {
      fields.push_back(value);
      ++record_no;
      return true;
    } else if (ch == '\r') {
      if (input.peek() == '\n') { input.get(); }
      fields.push_back(value);
      ++record_no;
      return true;
    } else {
      value.push_back(ch);
    }
  }
}

bool parse_int64(const std::string &s, int64_t &value)
{
  if (s.empty()) { return false; }
  errno = 0;
  char *end = nullptr;
  long long parsed = std::strtoll(s.c_str(), &end, 10);
  if (errno == ERANGE || end != s.c_str() + s.size()) { return false; }
  value = static_cast<int64_t>(parsed);
  return true;
}

bool parse_double(const std::string &s, double &value)
{
  if (s.empty()) { return false; }
  errno = 0;
  char *end = nullptr;
  value = std::strtod(s.c_str(), &end);
  return errno != ERANGE && end == s.c_str() + s.size();
}

bool parse_date(const std::string &s, int32_t &value)
{
  if (s.size() != 10 || s[4] != '-' || s[7] != '-') { return false; }
  return OB_SUCCESS == ObTimeConverter::str_to_date(ObString(s.size(), s.data()), value);
}

bool parse_datetime(const std::string &input, int64_t &value)
{
  if (input.size() < 19 || (input[10] != ' ' && input[10] != 'T')) { return false; }
  std::string s(input);
  s[10] = ' ';
  ObTimeConvertCtx context(nullptr, false);
  return OB_SUCCESS == ObTimeConverter::str_to_datetime(ObString(s.size(), s.data()), context, value);
}

InferredKind infer_text(const std::string &s)
{
  if (s.empty() || s == "NULL") { return IK_NULL; }
  if (s == "true" || s == "false" || s == "TRUE" || s == "FALSE") { return IK_BOOL; }
  int64_t i = 0;
  double d = 0;
  int32_t date = 0;
  int64_t datetime = 0;
  if (parse_int64(s, i)) { return IK_INT; }
  if (parse_double(s, d)) { return IK_DOUBLE; }
  if (parse_date(s, date)) { return IK_DATE; }
  if (parse_datetime(s, datetime)) { return IK_DATETIME; }
  return IK_VARCHAR;
}

InferredKind widen(InferredKind current, InferredKind value)
{
  if (current == IK_NULL) { return value; }
  if (value == IK_NULL || current == value) { return current; }
  if ((current == IK_INT && value == IK_DOUBLE) || (current == IK_DOUBLE && value == IK_INT)) { return IK_DOUBLE; }
  if ((current == IK_DATE && value == IK_DATETIME) || (current == IK_DATETIME && value == IK_DATE)) { return IK_DATETIME; }
  return IK_VARCHAR;
}

void set_data_type(InferredKind kind, ObDataType &type)
{
  switch (kind) {
    case IK_BOOL: type.set_obj_type(ObTinyIntType); break;
    case IK_INT: type.set_int(); break;
    case IK_DOUBLE: type.set_obj_type(ObDoubleType); break;
    case IK_DATE: type.set_obj_type(ObDateType); break;
    case IK_DATETIME: type.set_obj_type(ObDateTimeType); break;
    default:
      type.set_obj_type(ObVarcharType);
      type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObVarcharType]);
      break;
  }
}

int copy_name(ObIAllocator &allocator, const std::string &source, ObString &target)
{
  return ob_write_string(allocator, ObString(source.size(), source.data()), target);
}

std::string normalize_header(const std::string &raw, int64_t index,
                             const std::vector<std::string> &existing)
{
  std::string base = raw.empty() ? "column" + std::to_string(index + 1) : raw;
  std::string candidate = base;
  int suffix = 2;
  auto equal_ci = [](const std::string &a, const std::string &b) {
    if (a.size() != b.size()) { return false; }
    for (size_t i = 0; i < a.size(); ++i) {
      if (std::tolower(static_cast<unsigned char>(a[i])) !=
          std::tolower(static_cast<unsigned char>(b[i]))) { return false; }
    }
    return true;
  };
  bool duplicate = true;
  while (duplicate) {
    duplicate = false;
    for (const auto &name : existing) {
      if (equal_ci(name, candidate)) { duplicate = true; candidate = base + "__" + std::to_string(suffix++); break; }
    }
  }
  return candidate;
}

int json_kind(ObJsonNode *node, InferredKind &kind)
{
  int ret = OB_SUCCESS;
  if (node == nullptr || node->json_type() == ObJsonNodeType::J_NULL) { kind = IK_NULL; }
  else if (node->json_type() == ObJsonNodeType::J_BOOLEAN) { kind = IK_BOOL; }
  else if (node->json_type() == ObJsonNodeType::J_INT || node->json_type() == ObJsonNodeType::J_UINT) { kind = IK_INT; }
  else if (node->json_type() == ObJsonNodeType::J_DOUBLE || node->json_type() == ObJsonNodeType::J_DECIMAL) { kind = IK_DOUBLE; }
  else if (node->json_type() == ObJsonNodeType::J_STRING) {
    const ObString &text = static_cast<ObJsonString *>(node)->value();
    kind = infer_text(std::string(text.ptr(), text.length()));
  }
  else { ret = OB_NOT_SUPPORTED; }
  return ret;
}

int parse_json_object(ObIAllocator &allocator, const std::string &line, ObJsonObject *&object)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *base = nullptr;
  ObString text(line.size(), line.data());
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, text, ObJsonInType::JSON_TREE,
                                               ObJsonInType::JSON_TREE, base))) {
  } else if (base == nullptr || base->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_DATA;
  } else { object = static_cast<ObJsonObject *>(base); }
  return ret;
}

int infer_csv(ObIAllocator &allocator, const ObString &path, ObIArray<ObFileTableColumn> &columns)
{
  int ret = OB_SUCCESS;
  std::ifstream input(std::string(path.ptr(), path.length()), std::ios::binary);
  if (!input.is_open()) { return OB_FILE_NOT_OPENED; }
  int64_t record = 0;
  std::vector<std::string> fields;
  if (!read_csv_record(input, fields, record) || fields.empty()) { return OB_INVALID_DATA; }
  std::vector<std::string> names;
  std::vector<InferredKind> kinds(fields.size(), IK_NULL);
  std::vector<bool> nullable(fields.size(), false);
  for (size_t i = 0; i < fields.size(); ++i) { names.push_back(normalize_header(fields[i], i, names)); }
  while (read_csv_record(input, fields, record)) {
    if (fields.size() != names.size()) { return OB_INVALID_DATA; }
    for (size_t i = 0; i < fields.size(); ++i) {
      InferredKind value = infer_text(fields[i]);
      nullable[i] = nullable[i] || value == IK_NULL;
      kinds[i] = widen(kinds[i], value);
    }
  }
  for (size_t i = 0; OB_SUCC(ret) && i < names.size(); ++i) {
    ObFileTableColumn column;
    OZ (copy_name(allocator, names[i], column.name_));
    set_data_type(kinds[i], column.type_);
    column.nullable_ = nullable[i] || kinds[i] == IK_NULL;
    OZ (columns.push_back(column));
  }
  return ret;
}

int infer_jsonl(ObIAllocator &allocator, const ObString &path, ObIArray<ObFileTableColumn> &columns)
{
  int ret = OB_SUCCESS;
  std::ifstream input(std::string(path.ptr(), path.length()), std::ios::binary);
  if (!input.is_open()) { return OB_FILE_NOT_OPENED; }
  std::vector<std::string> names;
  std::vector<InferredKind> kinds;
  std::vector<bool> nullable;
  std::string line;
  int64_t rows = 0;
  while (OB_SUCC(ret) && std::getline(input, line)) {
    if (!line.empty() && line.back() == '\r') { line.pop_back(); }
    if (line.empty()) { continue; }
    ObArenaAllocator row_allocator("FileJsonInfer");
    ObJsonObject *object = nullptr;
    OZ (parse_json_object(row_allocator, line, object));
    std::vector<bool> present(names.size(), false);
    for (int64_t i = 0; OB_SUCC(ret) && i < object->element_count(); ++i) {
      ObString key;
      OZ (object->get_key(i, key));
      std::string name(key.ptr(), key.length());
      size_t position = names.size();
      for (size_t j = 0; j < names.size(); ++j) { if (names[j] == name) { position = j; break; } }
      if (position == names.size()) {
        names.push_back(name);
        kinds.push_back(IK_NULL);
        nullable.push_back(rows > 0);
        present.push_back(true);
      } else { present[position] = true; }
      InferredKind value = IK_NULL;
      OZ (json_kind(object->get_value(i), value));
      nullable[position] = nullable[position] || value == IK_NULL;
      kinds[position] = widen(kinds[position], value);
    }
    for (size_t i = 0; i < present.size(); ++i) { if (!present[i]) { nullable[i] = true; } }
    ++rows;
  }
  if (OB_SUCC(ret) && names.empty()) { ret = OB_INVALID_DATA; }
  for (size_t i = 0; OB_SUCC(ret) && i < names.size(); ++i) {
    ObFileTableColumn column;
    OZ (copy_name(allocator, names[i], column.name_));
    set_data_type(kinds[i], column.type_);
    column.nullable_ = nullable[i] || kinds[i] == IK_NULL;
    OZ (columns.push_back(column));
  }
  return ret;
}

int text_to_obj(const std::string &text, const ObObjType type, ObIAllocator &allocator, ObObj &value)
{
  int ret = OB_SUCCESS;
  if (text.empty() || text == "NULL") { value.set_null(); }
  else if (type == ObTinyIntType) { value.set_tinyint((text == "true" || text == "TRUE" || text == "1") ? 1 : 0); }
  else if (type == ObIntType) { int64_t v = 0; if (!parse_int64(text, v)) ret = OB_INVALID_DATA; else value.set_int(v); }
  else if (type == ObDoubleType) { double v = 0; if (!parse_double(text, v)) ret = OB_INVALID_DATA; else value.set_double(v); }
  else if (type == ObDateType) { int32_t v = 0; if (!parse_date(text, v)) ret = OB_INVALID_DATA; else value.set_date(v); }
  else if (type == ObDateTimeType) { int64_t v = 0; if (!parse_datetime(text, v)) ret = OB_INVALID_DATA; else value.set_datetime(v); }
  else {
    ObString copied;
    OZ (ob_write_string(allocator, ObString(text.size(), text.data()), copied));
    if (OB_SUCC(ret)) { value.set_varchar(copied); value.set_collation_type(CS_TYPE_UTF8MB4_BIN); }
  }
  return ret;
}

class FileTableScan final : public ITableFunctionScan
{
public:
  explicit FileTableScan(const TableFunctionScanContext &context)
    : allocator_(context.allocator_), arguments_(context.arguments_), projection_(context.column_ids_),
      format_(context.descriptor_.id_ == READ_CSV_FUNCTION_ID ? FILE_TABLE_CSV :
                                (context.descriptor_.id_ == READ_JSON_FUNCTION_ID ||
                                 context.descriptor_.id_ == READ_NDJSON_FUNCTION_ID) ? FILE_TABLE_JSONL :
                                context.descriptor_.id_ == READ_PARQUET_FUNCTION_ID ? FILE_TABLE_PARQUET : FILE_TABLE_GLOB),
      input_(), row_allocator_("FileTableRow"), record_(0) {}
  int open() override { return rescan(arguments_); }
  int rescan(const ObIArray<ObObj> &arguments) override
  {
    int ret = close();
    columns_.reuse();
    if (OB_FAIL(ret)) {
    } else if (arguments.empty() || arguments.at(0).is_null()) { ret = OB_INVALID_ARGUMENT; }
    else {
      path_ = arguments.at(0).get_string();
      OZ (ObFileScanUtils::infer_schema(allocator_, path_, format_, columns_));
      if (OB_SUCC(ret) && format_ != FILE_TABLE_GLOB) {
        input_.open(std::string(path_.ptr(), path_.length()), std::ios::binary);
        if (!input_.is_open()) { ret = OB_FILE_NOT_OPENED; }
        else if (format_ == FILE_TABLE_CSV) { std::vector<std::string> header; if (!read_csv_record(input_, header, record_)) ret = OB_INVALID_DATA; }
      }
#ifndef _WIN32
      if (OB_SUCC(ret) && format_ == FILE_TABLE_GLOB) {
        std::string pattern(path_.ptr(), path_.length());
        glob_t result = {};
        int rc = ::glob(pattern.c_str(), 0, nullptr, &result);
        if (rc != 0 && rc != GLOB_NOMATCH) { ret = OB_IO_ERROR; }
        for (size_t i = 0; OB_SUCC(ret) && i < result.gl_pathc; ++i) { paths_.push_back(result.gl_pathv[i]); }
        globfree(&result);
      }
#endif
    }
    return ret;
  }
  int get_next_row(ObNewRow *&row) override
  {
    int ret = OB_SUCCESS;
    row_allocator_.reuse();
    cells_.reuse();
    if (format_ == FILE_TABLE_GLOB) {
      if (position_ >= paths_.size()) { return OB_ITER_END; }
      ObObj value;
      OZ (text_to_obj(paths_[position_++], ObVarcharType, row_allocator_, value));
      OZ (cells_.push_back(value));
    } else if (format_ == FILE_TABLE_CSV) {
      std::vector<std::string> fields;
      if (!read_csv_record(input_, fields, record_)) { return input_.eof() ? OB_ITER_END : OB_INVALID_DATA; }
      if (fields.size() != static_cast<size_t>(columns_.count())) { return OB_INVALID_DATA; }
      for (int64_t i = 0; OB_SUCC(ret) && i < projection_.count(); ++i) {
        int64_t index = projection_.at(i) - OB_APP_MIN_COLUMN_ID;
        if (index < 0 || index >= columns_.count()) { ret = OB_ERR_UNEXPECTED; }
        else { ObObj value; OZ (text_to_obj(fields[index], columns_.at(index).type_.get_obj_type(), row_allocator_, value)); OZ (cells_.push_back(value)); }
      }
    } else if (format_ == FILE_TABLE_JSONL) {
      std::string line;
      do { if (!std::getline(input_, line)) return OB_ITER_END; if (!line.empty() && line.back() == '\r') line.pop_back(); } while (line.empty());
      ObArenaAllocator json_allocator("FileJsonRow");
      ObJsonObject *object = nullptr;
      OZ (parse_json_object(json_allocator, line, object));
      for (int64_t i = 0; OB_SUCC(ret) && i < projection_.count(); ++i) {
        int64_t index = projection_.at(i) - OB_APP_MIN_COLUMN_ID;
        ObObj value;
        ObJsonNode *node = index >= 0 && index < columns_.count() ? object->get_value(columns_.at(index).name_) : nullptr;
        if (node == nullptr || node->json_type() == ObJsonNodeType::J_NULL) { value.set_null(); }
        else if (node->json_type() == ObJsonNodeType::J_BOOLEAN) { value.set_tinyint(static_cast<ObJsonBoolean *>(node)->value()); }
        else if (node->json_type() == ObJsonNodeType::J_INT) { value.set_int(static_cast<ObJsonInt *>(node)->value()); }
        else if (node->json_type() == ObJsonNodeType::J_UINT) { value.set_int(static_cast<int64_t>(static_cast<ObJsonUint *>(node)->value())); }
        else if (node->json_type() == ObJsonNodeType::J_DOUBLE) { value.set_double(static_cast<ObJsonDouble *>(node)->value()); }
        else if (node->json_type() == ObJsonNodeType::J_STRING) {
          const ObString &text = static_cast<ObJsonString *>(node)->value();
          OZ (text_to_obj(std::string(text.ptr(), text.length()), columns_.at(index).type_.get_obj_type(), row_allocator_, value));
        }
        else { ret = OB_NOT_SUPPORTED; }
        OZ (cells_.push_back(value));
      }
    } else { ret = OB_NOT_SUPPORTED; }
    if (OB_SUCC(ret)) { row_.cells_ = cells_.get_data(); row_.count_ = cells_.count(); row = &row_; }
    return ret;
  }
  int close() override
  {
    if (input_.is_open()) { input_.close(); }
    paths_.clear();
    position_ = 0;
    record_ = 0;
    return OB_SUCCESS;
  }
private:
  ObIAllocator &allocator_;
  const ObIArray<ObObj> &arguments_;
  const ObIArray<uint64_t> &projection_;
  ObFileTableFormat format_;
  ObString path_;
  ObSEArray<ObFileTableColumn, 16> columns_;
  std::ifstream input_;
  std::vector<std::string> paths_;
  size_t position_ = 0;
  ObArenaAllocator row_allocator_;
  ObSEArray<ObObj, 16> cells_;
  ObNewRow row_;
  int64_t record_;
};

} // namespace

int ObFileScanUtils::canonicalize_path(ObIAllocator &allocator, const ObString &path, ObString &canonical)
{
  int ret = OB_SUCCESS;
  char buffer[PATH_MAX + 1] = {};
  std::string source(path.ptr(), path.length());
#ifdef _WIN32
  if (_fullpath(buffer, source.c_str(), PATH_MAX) == nullptr) { ret = OB_FILE_NOT_EXIST; }
#else
  if (::realpath(source.c_str(), buffer) == nullptr) { ret = OB_FILE_NOT_EXIST; }
#endif
  if (OB_SUCC(ret)) { OZ (ob_write_string(allocator, ObString::make_string(buffer), canonical)); }
  return ret;
}

int ObFileScanUtils::infer_schema(ObIAllocator &allocator, const ObString &path,
                                  ObFileTableFormat format, ObIArray<ObFileTableColumn> &columns)
{
  int ret = OB_SUCCESS;
  columns.reuse();
  if (format == FILE_TABLE_CSV) { ret = infer_csv(allocator, path, columns); }
  else if (format == FILE_TABLE_JSONL) { ret = infer_jsonl(allocator, path, columns); }
  else if (format == FILE_TABLE_GLOB) {
    ObFileTableColumn column;
    OZ (copy_name(allocator, "file", column.name_));
    set_data_type(IK_VARCHAR, column.type_);
    column.nullable_ = false;
    OZ (columns.push_back(column));
  } else { ret = OB_NOT_SUPPORTED; }
  return ret;
}

int create_file_table_scan(const TableFunctionScanContext &context, ITableFunctionScan *&scan)
{
  void *memory = context.allocator_.alloc(sizeof(FileTableScan));
  if (memory == nullptr) { return OB_ALLOCATE_MEMORY_FAILED; }
  scan = new (memory) FileTableScan(context);
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
