#include <avro.h>
#include <stdio.h>

int main() {
    printf("Avro C version: Testing avro-c library\n");
    
    // Simple test to ensure the library is linkable
    avro_schema_t schema = avro_schema_string();
    if (schema) {
        printf("Successfully created Avro string schema\n");
        avro_schema_decref(schema);
        return 0;
    }
    
    printf("Failed to create Avro schema\n");
    return 1;
}
