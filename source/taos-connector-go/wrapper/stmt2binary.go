package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

int
go_generate_stmt2_binds(char *data, uint32_t count, uint32_t field_count, uint32_t field_offset,
                   TAOS_STMT2_BIND *bind_struct,
                   TAOS_STMT2_BIND **bind_ptr, char *err_msg) {
  uint32_t *base_length = (uint32_t *) (data + field_offset);
  char *data_ptr = (char *) (base_length + count);
  for (int table_index = 0; table_index < count; table_index++) {
    bind_ptr[table_index] = bind_struct + table_index * field_count;
    char *bind_data_ptr;
    for (uint32_t field_index = 0; field_index < field_count; field_index++) {
      bind_data_ptr = data_ptr;
      TAOS_STMT2_BIND *bind = bind_ptr[table_index] + field_index;
      // total length
      uint32_t bind_data_totalLength = *(uint32_t *) bind_data_ptr;
      bind_data_ptr += 4;
      // buffer_type
      bind->buffer_type = *(int *) bind_data_ptr;
      bind_data_ptr += 4;
      // num
      bind->num = *(int *) bind_data_ptr;
      bind_data_ptr += 4;
      // is_null
      bind->is_null = (char *) bind_data_ptr;
      bind_data_ptr += bind->num;
      // have_length
      char have_length = *(char *) bind_data_ptr;
      bind_data_ptr += 1;
      if (have_length == 0) {
        bind->length = NULL;
      } else {
        bind->length = (int32_t *) bind_data_ptr;
        bind_data_ptr += bind->num * 4;
      }
      // buffer_length
      int32_t buffer_length = *(int32_t *) bind_data_ptr;
      bind_data_ptr += 4;
      // buffer
      if (buffer_length > 0) {
        bind->buffer = (void *) bind_data_ptr;
        bind_data_ptr += buffer_length;
      } else {
        bind->buffer = NULL;
      }
      // check bind data length
      if (bind_data_ptr - data_ptr != bind_data_totalLength) {
        snprintf(err_msg, 128, "bind data length error, tableIndex: %d, fieldIndex: %d", table_index, field_index);
        return -1;
      }
      data_ptr = bind_data_ptr;
    }
  }
  return 0;
}


int go_stmt2_bind_binary(TAOS_STMT2 *stmt, char *data, int32_t col_idx, char *err_msg) {
  uint32_t *header = (uint32_t *) data;
  uint32_t total_length = header[0];
  uint32_t count = header[1];
  uint32_t tag_count = header[2];
  uint32_t col_count = header[3];
  uint32_t table_names_offset = header[4];
  uint32_t tags_offset = header[5];
  uint32_t cols_offset = header[6];
  // check table names
  if (table_names_offset > 0) {
    uint32_t table_name_end = table_names_offset + count * 2;
    if (table_name_end > total_length) {
      snprintf(err_msg, 128, "table name lengths out of range, total length: %d, tableNamesLengthEnd: %d", total_length,
               table_name_end);
      return -1;
    }
    uint16_t *table_name_length_ptr = (uint16_t *) (data + table_names_offset);
    for (int32_t i = 0; i < count; ++i) {
      if (table_name_length_ptr[i] == 0) {
		snprintf(err_msg, 128, "table name length is 0, tableIndex: %d", i);
		return -1;
	  }
      table_name_end += (uint32_t) table_name_length_ptr[i];
    }
    if (table_name_end > total_length) {
      snprintf(err_msg, 128, "table names out of range, total length: %d, tableNameTotalLength: %d", total_length,
               table_name_end);
      return -1;
    }
  }
  // check tags
  if (tags_offset > 0) {
    if (tag_count == 0) {
      snprintf(err_msg, 128, "tag count is 0, but tags offset is not 0");
      return -1;
    }
    uint32_t tag_end = tags_offset + count * 4;
    if (tag_end > total_length) {
      snprintf(err_msg, 128, "tags out of range, total length: %d, tagEnd: %d", total_length, tag_end);
      return -1;
    }
    uint32_t *tab_length_ptr = (uint32_t *) (data + tags_offset);
    for (int32_t i = 0; i < count; ++i) {
      if (tab_length_ptr[i] == 0) {
        snprintf(err_msg, 128, "tag length is 0, tableIndex: %d", i);
        return -1;
      }
      tag_end += tab_length_ptr[i];
    }
    if (tag_end > total_length) {
      snprintf(err_msg, 128, "tags out of range, total length: %d, tagsTotalLength: %d", total_length, tag_end);
      return -1;
    }
  }
  // check cols
  if (cols_offset > 0) {
    if (col_count == 0) {
      snprintf(err_msg, 128, "col count is 0, but cols offset is not 0");
      return -1;
    }
    uint32_t colEnd = cols_offset + count * 4;
    if (colEnd > total_length) {
      snprintf(err_msg, 128, "cols out of range, total length: %d, colEnd: %d", total_length, colEnd);
      return -1;
    }
    uint32_t *col_length_ptr = (uint32_t *) (data + cols_offset);
    for (int32_t i = 0; i < count; ++i) {
      if (col_length_ptr[i] == 0) {
        snprintf(err_msg, 128, "col length is 0, tableIndex: %d", i);
        return -1;
      }
      colEnd += col_length_ptr[i];
    }
    if (colEnd > total_length) {
      snprintf(err_msg, 128, "cols out of range, total length: %d, colsTotalLength: %d", total_length, colEnd);
      return -1;
    }
  }
  // generate bindv struct
  TAOS_STMT2_BINDV bind_v;
  bind_v.count = (int) count;
  if (table_names_offset > 0) {
    uint16_t *table_name_length_ptr = (uint16_t *) (data + table_names_offset);
    char *table_name_data_ptr = (char *) (table_name_length_ptr) + 2 * count;
    char **table_name = (char **) malloc(sizeof(char *) * count);
    if (table_name == NULL) {
      snprintf(err_msg, 128, "malloc tableName error");
      return -1;
    }
    for (int i = 0; i < count; i++) {
      table_name[i] = table_name_data_ptr;
      table_name_data_ptr += table_name_length_ptr[i];
    }
    bind_v.tbnames = table_name;
  } else {
    bind_v.tbnames = NULL;
  }
  uint32_t bind_struct_count = 0;
  uint32_t bind_ptr_count = 0;
  if (tags_offset == 0) {
    bind_v.tags = NULL;
  } else {
    bind_struct_count += count * tag_count;
    bind_ptr_count += count;
  }
  if (cols_offset == 0) {
    bind_v.bind_cols = NULL;
  } else {
    bind_struct_count += count * col_count;
    bind_ptr_count += count;
  }
  TAOS_STMT2_BIND *bind_struct = NULL;
  TAOS_STMT2_BIND **bind_ptr = NULL;
  if (bind_struct_count == 0) {
    bind_v.tags = NULL;
    bind_v.bind_cols = NULL;
  } else {
    // []TAOS_STMT2_BIND bindStruct
    bind_struct = (TAOS_STMT2_BIND *) malloc(sizeof(TAOS_STMT2_BIND) * bind_struct_count);
    if (bind_struct == NULL) {
      snprintf(err_msg, 128, "malloc bind struct error");
      free(bind_v.tbnames);
      return -1;
    }
    // []TAOS_STMT2_BIND *bindPtr
    bind_ptr = (TAOS_STMT2_BIND **) malloc(sizeof(TAOS_STMT2_BIND *) * bind_ptr_count);
    if (bind_ptr == NULL) {
      snprintf(err_msg, 128, "malloc bind pointer error");
      free(bind_struct);
      free(bind_v.tbnames);
      return -1;
    }
    uint32_t struct_index = 0;
    uint32_t ptr_index = 0;
    if (tags_offset > 0) {
      int code = go_generate_stmt2_binds(data, count, tag_count, tags_offset, bind_struct, bind_ptr, err_msg);
      if (code != 0) {
        free(bind_struct);
        free(bind_ptr);
        free(bind_v.tbnames);
        return code;
      }
      bind_v.tags = bind_ptr;
      struct_index += count * tag_count;
      ptr_index += count;
    }
    if (cols_offset > 0) {
      TAOS_STMT2_BIND *col_bind_struct = bind_struct + struct_index;
      TAOS_STMT2_BIND **col_bind_ptr = bind_ptr + ptr_index;
      int code = go_generate_stmt2_binds(data, count, col_count, cols_offset, col_bind_struct, col_bind_ptr,
                                    err_msg);
      if (code != 0) {
        free(bind_struct);
        free(bind_ptr);
        free(bind_v.tbnames);
        return code;
      }
      bind_v.bind_cols = col_bind_ptr;
    }
  }
  int code = taos_stmt2_bind_param(stmt, &bind_v, col_idx);
  if (code != 0) {
    char *msg = taos_stmt2_error(stmt);
    snprintf(err_msg, 128, "%s", msg);
  }
  if (bind_v.tbnames != NULL) {
    free(bind_v.tbnames);
  }
  if (bind_struct != NULL) {
    free(bind_struct);
  }
  if (bind_ptr != NULL) {
    free(bind_ptr);
  }
  return code;
}
*/
import "C"
import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common/stmt"
	taosError "github.com/taosdata/driver-go/v3/errors"
)

// TaosStmt2BindBinary bind binary data to stmt2
func TaosStmt2BindBinary(stmt2 unsafe.Pointer, data []byte, colIdx int32) error {
	if len(data) < stmt.DataPosition {
		return fmt.Errorf("data length is less than 28")
	}
	totalLength := binary.LittleEndian.Uint32(data[stmt.TotalLengthPosition:])
	if totalLength != uint32(len(data)) {
		return fmt.Errorf("total length not match, expect %d, but get %d", len(data), totalLength)
	}
	dataP := C.CBytes(data)
	defer C.free(dataP)
	errMsg := (*C.char)(C.malloc(128))
	defer C.free(unsafe.Pointer(errMsg))

	code := C.go_stmt2_bind_binary(stmt2, (*C.char)(dataP), C.int32_t(colIdx), errMsg)
	if code != 0 {
		msg := C.GoString(errMsg)
		return taosError.NewError(int(code), msg)
	}
	return nil
}
