/**
 * TDengine License C SDK
 *
 * This SDK allows C applications to connect to CULS (Customer License Server)
 * and manage licenses through P2P networking.
 */

#ifndef TAOS_LICENSE_H
#define TAOS_LICENSE_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Error codes */
#define TAOS_LICENSE_OK              0
#define TAOS_LICENSE_ERROR          -1
#define TAOS_LICENSE_INVALID_PARAM  -2
#define TAOS_LICENSE_NO_LICENSE     -3
#define TAOS_LICENSE_EXPIRED        -4
#define TAOS_LICENSE_REVOKED        -5
#define TAOS_LICENSE_NETWORK_ERROR  -6
#define TAOS_LICENSE_VERIFY_FAILED  -7
#define TAOS_INSTANCE_BLACKLISTED    -8

/* License information structure */
typedef struct taos_license_info {
    char     license_id[64];
    char     culs_id[64];
    char     license_type[32];
    int64_t  valid_until;
    uint64_t timeseries_limit;
    uint32_t cpu_cores_limit;
    uint32_t dnodes_limit;
    uint64_t storage_limit;
    bool     feature_audit;
    bool     feature_data_sync;
    bool     feature_backup_restore;
} taos_license_info_t;

/* Opaque SDK handle */
typedef struct taos_sdk_handle taos_sdk_handle_t;

/**
 * Create a new SDK instance
 *
 * @param handle      Pointer to store the SDK handle (output)
 * @param culs_addr   CULS multiaddress (e.g., "/ip4/127.0.0.1/tcp/8087/p2p/12D3KooW...")
 * @param instance_id Instance ID (can be NULL for auto-generated UUID)
 * @return            Error code (TAOS_LICENSE_OK on success)
 */
int taos_sdk_create(taos_sdk_handle_t** handle, const char* culs_addr, const char* instance_id);

/**
 * Destroy an SDK instance
 *
 * @param handle  SDK handle returned by taos_sdk_create
 */
void taos_sdk_destroy(taos_sdk_handle_t* handle);

/**
 * Get license from CULS
 *
 * This function retrieves a license from CULS, verifies the signature,
 * and checks that the license is not expired or revoked.
 *
 * @param handle  SDK handle
 * @param info    Pointer to store license information (output)
 * @return        Error code (TAOS_LICENSE_OK on success)
 */
int taos_sdk_get_license(taos_sdk_handle_t* handle, taos_license_info_t* info);

/**
 * Send heartbeat to CULS
 *
 * This function sends a heartbeat message to CULS to indicate
 * that the instance is still active.
 *
 * @param handle  SDK handle
 * @return        Error code (TAOS_LICENSE_OK on success)
 */
int taos_sdk_heartbeat(taos_sdk_handle_t* handle);

/**
 * Revoke the current license
 *
 * This function requests CULS to revoke the license assigned to this instance.
 * Useful for testing license revocation scenarios.
 *
 * @param handle  SDK handle
 * @param reason  Reason for revocation (can be NULL)
 * @return        Error code (TAOS_LICENSE_OK on success)
 */
int taos_sdk_revoke_license(taos_sdk_handle_t* handle, const char* reason);

/**
 * Get error string for the last error
 *
 * Note: The returned string must be freed by calling taos_sdk_free_error_string()
 *
 * @param handle      SDK handle (can be NULL)
 * @param error_code  Error code
 * @return            Error message string (must be freed by caller)
 */
const char* taos_sdk_error_string(taos_sdk_handle_t* handle, int error_code);

/**
 * Free error string returned by taos_sdk_error_string
 *
 * @param s  Error string returned by taos_sdk_error_string
 */
void taos_sdk_free_error_string(const char* s);

#ifdef __cplusplus
}
#endif

#endif /* TAOS_LICENSE_H */
