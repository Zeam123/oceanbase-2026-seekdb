/*
 * Copyright (c) 2025 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */
#pragma once

#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
#include "sql/table_function/table_function.h"

namespace oceanbase {
namespace sql {

enum ObFileTableFormat
{
  FILE_TABLE_CSV = 0,
  FILE_TABLE_JSONL,
  FILE_TABLE_PARQUET,
  FILE_TABLE_GLOB
};

struct ObFileTableColumn
{
  common::ObString name_;
  common::ObDataType type_;
  bool nullable_;
};

class ObFileScanUtils
{
public:
  static int canonicalize_path(common::ObIAllocator &allocator,
                               const common::ObString &path,
                               common::ObString &canonical);
  static int infer_schema(common::ObIAllocator &allocator,
                          const common::ObString &path,
                          ObFileTableFormat format,
                          common::ObIArray<ObFileTableColumn> &columns);
};

int create_file_table_scan(const TableFunctionScanContext &context,
                           ITableFunctionScan *&scan);

} // namespace sql
} // namespace oceanbase
