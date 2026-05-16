/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _stmt_h_
#define _stmt_h_

#include "macros.h"
#include "typedefs.h"

EXTERN_C_BEGIN

stmt_t* stmt_create(conn_t *conn) FA_HIDDEN;
stmt_t* stmt_ref(stmt_t *stmt) FA_HIDDEN;
stmt_t* stmt_unref(stmt_t *stmt) FA_HIDDEN;
SQLRETURN stmt_free(stmt_t *stmt) FA_HIDDEN;
void stmt_clr_errs(stmt_t *stmt) FA_HIDDEN;

descriptor_t* stmt_APD(stmt_t *stmt) FA_HIDDEN;
descriptor_t* stmt_IPD(stmt_t *stmt) FA_HIDDEN;

SQLRETURN stmt_exec_direct(stmt_t *stmt, SQLCHAR *StatementText, SQLINTEGER TextLength) FA_HIDDEN;
SQLRETURN stmt_get_row_count(stmt_t *stmt, SQLLEN *row_count_ptr) FA_HIDDEN;
SQLRETURN stmt_get_col_count(stmt_t *stmt, SQLSMALLINT *col_count_ptr) FA_HIDDEN;

SQLRETURN stmt_describe_col(stmt_t *stmt,
    SQLUSMALLINT   ColumnNumber,
    SQLCHAR       *ColumnName,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *NameLengthPtr,
    SQLSMALLINT   *DataTypePtr,
    SQLULEN       *ColumnSizePtr,
    SQLSMALLINT   *DecimalDigitsPtr,
    SQLSMALLINT   *NullablePtr) FA_HIDDEN;
SQLRETURN stmt_bind_col(stmt_t *stmt,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr) FA_HIDDEN;

SQLRETURN stmt_fetch(stmt_t *stmt) FA_HIDDEN;

SQLRETURN stmt_fetch_scroll(stmt_t *stmt,
    SQLSMALLINT   FetchOrientation,
    SQLLEN        FetchOffset) FA_HIDDEN;

SQLRETURN stmt_get_diag_rec(
    stmt_t         *stmt,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr) FA_HIDDEN;

SQLRETURN stmt_get_data(
    stmt_t        *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr) FA_HIDDEN;

SQLRETURN stmt_prepare(stmt_t *stmt,
    SQLCHAR      *StatementText,
    SQLINTEGER    TextLength) FA_HIDDEN;

SQLRETURN stmt_get_num_params(
    stmt_t         *stmt,
    SQLSMALLINT    *ParameterCountPtr) FA_HIDDEN;

SQLRETURN stmt_describe_param(
    stmt_t         *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr) FA_HIDDEN;

SQLRETURN stmt_bind_param(
    stmt_t         *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ValueType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr) FA_HIDDEN;

SQLRETURN stmt_execute(
    stmt_t         *stmt) FA_HIDDEN;

void stmt_dissociate_APD(stmt_t *stmt) FA_HIDDEN;
void stmt_dissociate_ARD(stmt_t *stmt) FA_HIDDEN;

SQLRETURN stmt_set_attr(stmt_t *stmt, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength) FA_HIDDEN;

SQLRETURN stmt_free_stmt(stmt_t *stmt, SQLUSMALLINT Option) FA_HIDDEN;
SQLRETURN stmt_close_cursor(stmt_t *stmt) FA_HIDDEN;

SQLRETURN stmt_tables(stmt_t *stmt,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *TableType,
    SQLSMALLINT    NameLength4) FA_HIDDEN;

#if (ODBCVER >= 0x0300)          /* { */
SQLRETURN stmt_get_attr(stmt_t *stmt,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength) FA_HIDDEN;
#endif                           /* } */

SQLRETURN stmt_get_diag_field(
    stmt_t         *stmt,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr) FA_HIDDEN;

SQLRETURN stmt_col_attribute(
    stmt_t         *stmt,
    SQLUSMALLINT    ColumnNumber,
    SQLUSMALLINT    FieldIdentifier,
    SQLPOINTER      CharacterAttributePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr,
    SQLLEN         *NumericAttributePtr) FA_HIDDEN;

SQLRETURN stmt_more_results(
    stmt_t         *stmt) FA_HIDDEN;

SQLRETURN stmt_columns(
    stmt_t         *stmt,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLCHAR *ColumnName, SQLSMALLINT NameLength4) FA_HIDDEN;

#if (ODBCVER >= 0x0300)       /* { */
SQLRETURN stmt_bulk_operations(
    stmt_t             *stmt,
    SQLSMALLINT         Operation) FA_HIDDEN;
#endif                        /* } */

SQLRETURN stmt_column_privileges(
    stmt_t       *stmt,
    SQLCHAR      *CatalogName,
    SQLSMALLINT   NameLength1,
    SQLCHAR      *SchemaName,
    SQLSMALLINT   NameLength2,
    SQLCHAR      *TableName,
    SQLSMALLINT   NameLength3,
    SQLCHAR      *ColumnName,
    SQLSMALLINT   NameLength4) FA_HIDDEN;

SQLRETURN stmt_extended_fetch(
    stmt_t          *stmt,
    SQLUSMALLINT     FetchOrientation,
    SQLLEN           FetchOffset,
    SQLULEN         *RowCountPtr,
    SQLUSMALLINT    *RowStatusArray) FA_HIDDEN;

SQLRETURN stmt_foreign_keys(
    stmt_t        *stmt,
    SQLCHAR       *PKCatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *PKSchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *PKTableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *FKCatalogName,
    SQLSMALLINT    NameLength4,
    SQLCHAR       *FKSchemaName,
    SQLSMALLINT    NameLength5,
    SQLCHAR       *FKTableName,
    SQLSMALLINT    NameLength6) FA_HIDDEN;

SQLRETURN stmt_get_cursor_name(
    stmt_t       *stmt,
    SQLCHAR      *CursorName,
    SQLSMALLINT   BufferLength,
    SQLSMALLINT  *NameLengthPtr) FA_HIDDEN;

SQLRETURN stmt_get_type_info(
    stmt_t       *stmt,
    SQLSMALLINT   DataType) FA_HIDDEN;

SQLRETURN stmt_param_data(
    stmt_t       *stmt,
    SQLPOINTER   *Value) FA_HIDDEN;

SQLRETURN stmt_primary_keys(
    stmt_t        *stmt,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3) FA_HIDDEN;

SQLRETURN stmt_procedure_columns(
    stmt_t       *stmt,
    SQLCHAR      *CatalogName,
    SQLSMALLINT   NameLength1,
    SQLCHAR      *SchemaName,
    SQLSMALLINT   NameLength2,
    SQLCHAR      *ProcName,
    SQLSMALLINT   NameLength3,
    SQLCHAR      *ColumnName,
    SQLSMALLINT   NameLength4) FA_HIDDEN;

SQLRETURN stmt_procedures(
    stmt_t         *stmt,
    SQLCHAR        *CatalogName,
    SQLSMALLINT     NameLength1,
    SQLCHAR        *SchemaName,
    SQLSMALLINT     NameLength2,
    SQLCHAR        *ProcName,
    SQLSMALLINT     NameLength3) FA_HIDDEN;

SQLRETURN stmt_put_data(
    stmt_t         *stmt,
    SQLPOINTER      Data,
    SQLLEN          StrLen_or_Ind) FA_HIDDEN;

SQLRETURN stmt_set_cursor_name(
    stmt_t         *stmt,
    SQLCHAR        *CursorName,
    SQLSMALLINT     NameLength) FA_HIDDEN;

SQLRETURN stmt_set_pos(
    stmt_t         *stmt,
    SQLSETPOSIROW   RowNumber,
    SQLUSMALLINT    Operation,
    SQLUSMALLINT    LockType) FA_HIDDEN;

SQLRETURN stmt_special_columns(
    stmt_t         *stmt,
    SQLUSMALLINT    IdentifierType,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLUSMALLINT Scope, SQLUSMALLINT Nullable) FA_HIDDEN;

SQLRETURN stmt_statistics(
    stmt_t  *stmt,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLUSMALLINT Unique, SQLUSMALLINT Reserved) FA_HIDDEN;

SQLRETURN stmt_table_privileges(
    stmt_t  *stmt,
    SQLCHAR *CatalogName,
    SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName,
    SQLSMALLINT NameLength2,
    SQLCHAR *TableName,
    SQLSMALLINT NameLength3) FA_HIDDEN;

SQLRETURN stmt_complete_async(
    stmt_t      *stmt,
    RETCODE     *AsyncRetCodePtr) FA_HIDDEN;

EXTERN_C_END

#endif //  _stmt_h_

