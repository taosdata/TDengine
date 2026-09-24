/* Strict parser and validator for the copied external fixture contract. */

#define _POSIX_C_SOURCE 200809L

#include "qst_mapping.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "toolscJson.h"

enum {
  QST_MAPPING_FILE_MAX_BYTES = 1024 * 1024,
  QST_MAPPING_TARGET_COUNT = 3,
  QST_MAPPING_TABLE_COUNT = 3,
  QST_MAPPING_PRIMARY_KEY_COUNT = 2,
};

typedef struct {
  const char *target;
  const char *script;
  const char *tables[QST_MAPPING_TABLE_COUNT];
  const char *primary_key[QST_MAPPING_PRIMARY_KEY_COUNT];
  const char *time_precision;
  const char *null_semantics;
  const char *cleanup;
  const char *type_mapping;
} SQstExpectedTarget;

static const SQstExpectedTarget kQstExpectedTargets[QST_MAPPING_TARGET_COUNT] = {
    {"mysql", "prepare-mysql-data.sh",
     {"fq_series", "fq_device", "fq_series_empty"}, {"ts", "device_id"},
     "us", "sql_null", "drop_owned_databases", "mysql-v1"},
    {"pg", "prepare-postgresql-data.sh",
     {"fq_series", "fq_device", "fq_series_empty"}, {"ts", "device_id"},
     "us", "sql_null", "drop_owned_database", "postgresql-v1"},
    {"influx", "prepare-influxdb-data.sh",
     {"fq_series", "fq_device", "fq_precision"}, {"time", "device_id"},
     "ns", "missing_field", "delete_owned_buckets", "influxdb-v3-arrow-v1"},
};

static const char *const kQstSchemaRootKeys[] = {
    "$schema", "type", "required", "properties", "additionalProperties",
};
static const char *const kQstRulesRootKeys[] = {
    "version", "dataset_schema", "targets",
};
static const char *const kQstTargetKeys[] = {
    "target", "script", "namespace", "tables", "primary_key", "timezone",
    "time_precision", "null_semantics", "cleanup", "type_mapping",
};

static int32_t qstMappingIsString(const tools_cJSON *item, const char *value) {
  return NULL != item && tools_cJSON_IsString(item) &&
         NULL != item->valuestring && strcmp(item->valuestring, value) == 0;
}

static int32_t qstMappingHasExpectedKeys(const tools_cJSON *object,
                                         const char *const *keys,
                                         uint32_t key_count) {
  uint32_t found = 0;

  if (NULL == object || !tools_cJSON_IsObject(object)) {
    return 0;
  }
  for (const tools_cJSON *item = object->child; NULL != item; item = item->next) {
    uint32_t index = 0;

    if (NULL == item->string) {
      return 0;
    }
    for (; index < key_count; ++index) {
      if (strcmp(item->string, keys[index]) == 0) {
        break;
      }
    }
    if (index == key_count || (found & (UINT32_C(1) << index)) != 0) {
      return 0;
    }
    found |= UINT32_C(1) << index;
  }
  return found == ((UINT32_C(1) << key_count) - 1);
}

static const tools_cJSON *qstMappingObjectItem(const tools_cJSON *object,
                                                const char *name) {
  const tools_cJSON *item = NULL;

  if (NULL == object || NULL == name) {
    return NULL;
  }
  item = tools_cJSON_GetObjectItemCaseSensitive(object, name);
  return item;
}

static int32_t qstMappingStringArrayEquals(const tools_cJSON *array,
                                            const char *const *expected,
                                            uint32_t expected_count) {
  if (NULL == array || !tools_cJSON_IsArray(array) ||
      tools_cJSON_GetArraySize(array) != (int32_t)expected_count) {
    return 0;
  }
  for (uint32_t index = 0; index < expected_count; ++index) {
    if (!qstMappingIsString(tools_cJSON_GetArrayItem(array, (int32_t)index),
                            expected[index])) {
      return 0;
    }
  }
  return 1;
}

static EQstCode qstMappingReadFile(const char *path, char **out_bytes) {
  struct stat status = {0};
  FILE *file = NULL;
  char *bytes = NULL;
  size_t count = 0;
  int32_t close_result = 0;

  if (NULL == path || NULL == out_bytes || stat(path, &status) != 0 ||
      !S_ISREG(status.st_mode) || status.st_size <= 0 ||
      (uint64_t)status.st_size > QST_MAPPING_FILE_MAX_BYTES) {
    return QST_E_INVALID_ARGUMENT;
  }
  bytes = (char *)calloc((size_t)status.st_size + 1, 1);
  if (NULL == bytes) {
    return QST_E_RESOURCE;
  }
  file = fopen(path, "rb");
  if (NULL == file) {
    free(bytes);
    return QST_E_RESOURCE;
  }
  count = fread(bytes, 1, (size_t)status.st_size, file);
  close_result = fclose(file);
  if (count != (size_t)status.st_size || close_result != 0 ||
      memchr(bytes, '\0', count) != NULL) {
    free(bytes);
    return QST_E_RESOURCE;
  }
  *out_bytes = bytes;
  return QST_OK;
}

static int32_t qstMappingPropertyHasConst(const tools_cJSON *properties,
                                          const char *name,
                                          const char *string_value,
                                          int32_t number_value) {
  const tools_cJSON *property = qstMappingObjectItem(properties, name);
  const tools_cJSON *value = qstMappingObjectItem(property, "const");

  if (NULL == property || !tools_cJSON_IsObject(property) || NULL == value) {
    return 0;
  }
  return NULL != string_value ? qstMappingIsString(value, string_value) :
      tools_cJSON_IsNumber(value) && value->valuedouble == number_value;
}

static int32_t qstMappingPropertyHasType(const tools_cJSON *properties,
                                         const char *name,
                                         const char *type_name) {
  const tools_cJSON *property = qstMappingObjectItem(properties, name);

  return NULL != property && tools_cJSON_IsObject(property) &&
         qstMappingIsString(qstMappingObjectItem(property, "type"), type_name);
}

static int32_t qstMappingPropertyHasEnum(const tools_cJSON *properties,
                                         const char *name,
                                         const char *const *values,
                                         uint32_t value_count) {
  const tools_cJSON *property = qstMappingObjectItem(properties, name);

  return NULL != property && tools_cJSON_IsObject(property) &&
         qstMappingStringArrayEquals(qstMappingObjectItem(property, "enum"),
                                     values, value_count);
}

static int32_t qstMappingPropertyHasStringArray(const tools_cJSON *properties,
                                                const char *name,
                                                uint32_t item_count) {
  const tools_cJSON *property = qstMappingObjectItem(properties, name);
  const tools_cJSON *items = qstMappingObjectItem(property, "items");

  return NULL != property && tools_cJSON_IsObject(property) &&
         qstMappingIsString(qstMappingObjectItem(property, "type"), "array") &&
         tools_cJSON_IsNumber(qstMappingObjectItem(property, "minItems")) &&
         tools_cJSON_IsNumber(qstMappingObjectItem(property, "maxItems")) &&
         qstMappingObjectItem(property, "minItems")->valuedouble == item_count &&
         qstMappingObjectItem(property, "maxItems")->valuedouble == item_count &&
         NULL != items && tools_cJSON_IsObject(items) &&
         qstMappingIsString(qstMappingObjectItem(items, "type"), "string");
}

static int32_t qstMappingSchemaIsValid(const tools_cJSON *schema) {
  static const char *const target_values[] = {"mysql", "pg", "influx"};
  static const char *const precision_values[] = {"us", "ns"};
  static const char *const null_values[] = {"sql_null", "missing_field"};
  static const char *const cleanup_values[] = {
      "drop_owned_databases", "drop_owned_database", "delete_owned_buckets",
  };
  const tools_cJSON *required = NULL;
  const tools_cJSON *properties = NULL;
  const tools_cJSON *targets = NULL;
  const tools_cJSON *items = NULL;
  const tools_cJSON *target_required = NULL;
  const tools_cJSON *target_properties = NULL;

  if (!qstMappingHasExpectedKeys(schema, kQstSchemaRootKeys,
                                 sizeof(kQstSchemaRootKeys) /
                                     sizeof(kQstSchemaRootKeys[0])) ||
      !qstMappingIsString(qstMappingObjectItem(schema, "$schema"),
                          "https://json-schema.org/draft/2020-12/schema") ||
      !qstMappingIsString(qstMappingObjectItem(schema, "type"), "object") ||
      !tools_cJSON_IsFalse(qstMappingObjectItem(schema,
                                                 "additionalProperties"))) {
    return 0;
  }
  required = qstMappingObjectItem(schema, "required");
  properties = qstMappingObjectItem(schema, "properties");
  if (!qstMappingStringArrayEquals(required, kQstRulesRootKeys,
                                   sizeof(kQstRulesRootKeys) /
                                       sizeof(kQstRulesRootKeys[0])) ||
      !qstMappingHasExpectedKeys(properties, kQstRulesRootKeys,
                                 sizeof(kQstRulesRootKeys) /
                                     sizeof(kQstRulesRootKeys[0])) ||
      !qstMappingPropertyHasConst(properties, "version", NULL, 1) ||
      !qstMappingPropertyHasConst(properties, "dataset_schema",
                                  "taosQueryTest-default-v1", 0)) {
    return 0;
  }
  targets = qstMappingObjectItem(properties, "targets");
  items = qstMappingObjectItem(targets, "items");
  if (NULL == targets || !tools_cJSON_IsObject(targets) || NULL == items ||
      !tools_cJSON_IsObject(items) ||
      !qstMappingIsString(qstMappingObjectItem(targets, "type"), "array") ||
      !tools_cJSON_IsNumber(qstMappingObjectItem(targets, "minItems")) ||
      !tools_cJSON_IsNumber(qstMappingObjectItem(targets, "maxItems")) ||
      qstMappingObjectItem(targets, "minItems")->valuedouble != 3 ||
      qstMappingObjectItem(targets, "maxItems")->valuedouble != 3 ||
      !qstMappingIsString(qstMappingObjectItem(items, "type"), "object") ||
      !tools_cJSON_IsFalse(qstMappingObjectItem(items,
                                                 "additionalProperties"))) {
    return 0;
  }
  target_required = qstMappingObjectItem(items, "required");
  target_properties = qstMappingObjectItem(items, "properties");
  return qstMappingStringArrayEquals(
             target_required, kQstTargetKeys,
             sizeof(kQstTargetKeys) / sizeof(kQstTargetKeys[0])) &&
         qstMappingHasExpectedKeys(
             target_properties, kQstTargetKeys,
             sizeof(kQstTargetKeys) / sizeof(kQstTargetKeys[0])) &&
         qstMappingPropertyHasEnum(target_properties, "target", target_values,
                                   sizeof(target_values) / sizeof(target_values[0])) &&
         qstMappingPropertyHasType(target_properties, "script", "string") &&
         qstMappingPropertyHasConst(target_properties, "namespace", "fq_default", 0) &&
         qstMappingPropertyHasStringArray(target_properties, "tables",
                                          QST_MAPPING_TABLE_COUNT) &&
         qstMappingPropertyHasStringArray(target_properties, "primary_key",
                                          QST_MAPPING_PRIMARY_KEY_COUNT) &&
         qstMappingPropertyHasConst(target_properties, "timezone", "UTC", 0) &&
         qstMappingPropertyHasEnum(target_properties, "time_precision",
                                   precision_values,
                                   sizeof(precision_values) /
                                       sizeof(precision_values[0])) &&
         qstMappingPropertyHasEnum(target_properties, "null_semantics",
                                   null_values,
                                   sizeof(null_values) / sizeof(null_values[0])) &&
         qstMappingPropertyHasEnum(target_properties, "cleanup", cleanup_values,
                                   sizeof(cleanup_values) /
                                       sizeof(cleanup_values[0])) &&
         qstMappingPropertyHasType(target_properties, "type_mapping", "string");
}

static int32_t qstMappingTargetIsValid(const tools_cJSON *target,
                                       const SQstExpectedTarget *expected) {
  if (!qstMappingHasExpectedKeys(target, kQstTargetKeys,
                                 sizeof(kQstTargetKeys) /
                                     sizeof(kQstTargetKeys[0])) ||
      !qstMappingIsString(qstMappingObjectItem(target, "target"),
                          expected->target) ||
      !qstMappingIsString(qstMappingObjectItem(target, "script"),
                          expected->script) ||
      !qstMappingIsString(qstMappingObjectItem(target, "namespace"),
                          "fq_default") ||
      !qstMappingStringArrayEquals(qstMappingObjectItem(target, "tables"),
                                   expected->tables, QST_MAPPING_TABLE_COUNT) ||
      !qstMappingStringArrayEquals(qstMappingObjectItem(target, "primary_key"),
                                   expected->primary_key,
                                   QST_MAPPING_PRIMARY_KEY_COUNT) ||
      !qstMappingIsString(qstMappingObjectItem(target, "timezone"), "UTC") ||
      !qstMappingIsString(qstMappingObjectItem(target, "time_precision"),
                          expected->time_precision) ||
      !qstMappingIsString(qstMappingObjectItem(target, "null_semantics"),
                          expected->null_semantics) ||
      !qstMappingIsString(qstMappingObjectItem(target, "cleanup"),
                          expected->cleanup) ||
      !qstMappingIsString(qstMappingObjectItem(target, "type_mapping"),
                          expected->type_mapping)) {
    return 0;
  }
  return 1;
}

static int32_t qstMappingRulesAreValid(const tools_cJSON *rules) {
  const tools_cJSON *targets = NULL;

  if (!qstMappingHasExpectedKeys(rules, kQstRulesRootKeys,
                                 sizeof(kQstRulesRootKeys) /
                                     sizeof(kQstRulesRootKeys[0])) ||
      !tools_cJSON_IsNumber(qstMappingObjectItem(rules, "version")) ||
      qstMappingObjectItem(rules, "version")->valuedouble != 1 ||
      !qstMappingIsString(qstMappingObjectItem(rules, "dataset_schema"),
                          "taosQueryTest-default-v1")) {
    return 0;
  }
  targets = qstMappingObjectItem(rules, "targets");
  if (NULL == targets || !tools_cJSON_IsArray(targets) ||
      tools_cJSON_GetArraySize(targets) != QST_MAPPING_TARGET_COUNT) {
    return 0;
  }
  for (uint32_t index = 0; index < QST_MAPPING_TARGET_COUNT; ++index) {
    if (!qstMappingTargetIsValid(
            tools_cJSON_GetArrayItem(targets, (int32_t)index),
            &kQstExpectedTargets[index])) {
      return 0;
    }
  }
  return 1;
}

EQstCode qst_mapping_validate_files(const char *rules_path,
                                    const char *schema_path) {
  char *rules_bytes = NULL;
  char *schema_bytes = NULL;
  tools_cJSON *rules = NULL;
  tools_cJSON *schema = NULL;
  EQstCode code = QST_E_INVALID_ARGUMENT;

  code = qstMappingReadFile(rules_path, &rules_bytes);
  if (code != QST_OK) {
    return code;
  }
  code = qstMappingReadFile(schema_path, &schema_bytes);
  if (code != QST_OK) {
    free(rules_bytes);
    return code;
  }
  code = QST_E_INVALID_ARGUMENT;
  rules = tools_cJSON_ParseWithOpts(rules_bytes, NULL, 1);
  schema = tools_cJSON_ParseWithOpts(schema_bytes, NULL, 1);
  if (NULL != rules && NULL != schema && qstMappingSchemaIsValid(schema) &&
      qstMappingRulesAreValid(rules)) {
    code = QST_OK;
  }
  tools_cJSON_Delete(rules);
  tools_cJSON_Delete(schema);
  free(rules_bytes);
  free(schema_bytes);
  return code;
}
