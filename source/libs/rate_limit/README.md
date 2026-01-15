# TDengine Rate Limiting Module

## Overview

This module implements comprehensive rate limiting functionality to protect TDengine from Denial of Service (DoS) attacks and resource exhaustion. It provides multiple algorithms for different use cases and can limit traffic at global, IP, user, and database levels.

## Features

### 1. Rate Limiting Algorithms

#### Token Bucket Algorithm
- **Use Case**: Smooth rate limiting for connections
- **Characteristics**: Allows bursts up to capacity, then enforces average rate
- **Example**: If capacity=100 and rate=10/sec, allows up to 100 requests instantly, then 10/sec

#### Leaky Bucket Algorithm
- **Use Case**: Traffic shaping for write operations
- **Characteristics**: Enforces strict rate, discards excess
- **Example**: If capacity=100 and leak rate=10/sec, accepts 10 requests/sec, discards excess

#### Sliding Window Algorithm
- **Use Case**: Precise rate limiting for queries
- **Characteristics**: Tracks request count over time window
- **Example**: If window=60s and maxCount=600, allows exactly 10 requests/sec over any 60s period

### 2. Multi-Level Rate Limiting

The module supports rate limiting at multiple levels:

- **Global Limit**: Server-wide limits for all connections
- **IP-based Limits**: Per-IP connection and query limits
- **User-based Limits**: Per-user query and write limits
- **Database Limits**: Per-database query and write limits (future)

## API Reference

### Initialization

```c
#include "rateLimitMgr.h"

// Initialize rate limiting module
// globalConnRate: Max connections per second (default: 10000)
// globalQueryRate: Max queries per second (default: 10000)
// globalWriteRate: Max writes per second (default: 10000)
int32_t rlmInit(int64_t globalConnRate, int64_t globalQueryRate, int64_t globalWriteRate);

// Cleanup and free all resources
void rlmCleanup();
```

### Connection Management

```c
// Check if a connection is allowed for given IP
// Returns: 0 = allowed, -1 = not initialized, -2 = global limit exceeded,
//          -3 = IP limiter creation failed, -4 = IP limit exceeded
int32_t rlmAllowConnection(const char* ip);

// Notify that a connection has closed (must be called)
void rlmConnectionClosed(const char* ip);
```

### Query/Write Control

```c
// Check if a query is allowed
// user, db, ip: Optional parameters (NULL to skip that level)
// Returns: 0 = allowed, negative values indicate which limit was exceeded
int32_t rlmAllowQuery(const char* user, const char* db, const char* ip);

// Check if a write operation is allowed
// user, db, ip: Optional parameters (NULL to skip that level)
int32_t rlmAllowWrite(const char* user, const char* db, const char* ip);
```

### Statistics and Management

```c
// Reset rate limiter statistics
// target: Which limiter to reset (global, IP, user, or DB)
// identifier: IP address, username, or DB name (NULL for global)
void rlmResetLimiter(ERLimitTarget target, const char* identifier);

// Get rate limiting statistics
// allowed: Output parameter for allowed request count
// rejected: Output parameter for rejected request count
void rlmGetStats(ERLimitTarget target, const char* identifier,
                 int64_t* allowed, int64_t* rejected);
```

## Usage Examples

### Example 1: Basic Connection Limiting

```c
// Initialize with default rates
if (rlmInit(10000, 10000, 10000) != 0) {
    fprintf(stderr, "Failed to initialize rate limiting\n");
    return -1;
}

// Handle new connection
const char* clientIp = "192.168.1.100";
int32_t ret = rlmAllowConnection(clientIp);
if (ret == 0) {
    // Connection allowed
    printf("Connection accepted from %s\n", clientIp);
} else {
    // Connection rejected
    printf("Connection rejected from %s (error: %d)\n", clientIp, ret);
}

// When connection closes
rlmConnectionClosed(clientIp);

// Cleanup
rlmCleanup();
```

### Example 2: Multi-Level Query Limiting

```c
// Check query permission
const char* username = "app_user";
const char* database = "sensors_db";
const char* clientIp = "192.168.1.100";

int32_t ret = rlmAllowQuery(username, database, clientIp);
switch (ret) {
    case 0:
        // Query allowed
        executeQuery();
        break;
    case -1:
        printf("Rate limiter not initialized\n");
        break;
    case -2:
        printf("Global query rate limit exceeded\n");
        break;
    case -3:
        printf("IP query rate limit exceeded\n");
        break;
    case -4:
        printf("User query rate limit exceeded\n");
        break;
    default:
        printf("Unknown error: %d\n", ret);
}
```

### Example 3: Monitoring Statistics

```c
// Get global statistics
int64_t allowed = 0, rejected = 0;
rlmGetStats(RLM_TARGET_GLOBAL, NULL, &allowed, &rejected);
printf("Global queries: %ld allowed, %ld rejected\n", allowed, rejected);

// Get per-IP statistics
const char* ip = "192.168.1.100";
rlmGetStats(RLM_TARGET_IP, ip, &allowed, &rejected);
printf("IP %s: %ld allowed, %ld rejected\n", ip, allowed, rejected);

// Get per-user statistics
const char* user = "app_user";
rlmGetStats(RLM_TARGET_USER, user, &allowed, &rejected);
printf("User %s: %ld allowed, %ld rejected\n", user, allowed, rejected);
```

## Integration with Transport Layer

To integrate rate limiting into the transport layer, add the following calls:

1. **During Connection Accept**:
```c
int32_t handleNewConnection(const char* ip) {
    if (rlmAllowConnection(ip) != 0) {
        return -1;  // Reject connection
    }
    // Proceed with connection setup
    return 0;
}
```

2. **During Connection Close**:
```c
void handleConnectionClose(const char* ip) {
    rlmConnectionClosed(ip);
    // Cleanup connection resources
}
```

3. **Before Processing Queries**:
```c
int32_t processQuery(const char* user, const char* db, const char* ip) {
    if (rlmAllowQuery(user, db, ip) != 0) {
        return TSDB_CODE_RATE_LIMIT_EXCEEDED;
    }
    // Process query
    return executeQuery();
}
```

## Configuration

### Default Values

```c
#define DEFAULT_CONN_RATE   10000   // Max 10,000 connections/second
#define DEFAULT_QUERY_RATE  10000   // Max 10,000 queries/second
#define DEFAULT_WRITE_RATE  10000   // Max 10,000 writes/second
#define MAX_IP_ENTRIES     1000      // Track up to 1000 unique IPs
```

### Customizing Limits

To customize limits for your environment:

```c
// Production: Higher limits
rlmInit(50000, 50000, 50000);

// Testing: Lower limits
rlmInit(100, 100, 100);

// Mixed: High connection limit, low query limit
rlmInit(50000, 1000, 5000);
```

## Best Practices

1. **Initialize Once**: Call `rlmInit()` during server startup
2. **Cleanup Properly**: Call `rlmCleanup()` during server shutdown
3. **Handle All Returns**: Check return codes for proper error handling
4. **Monitor Statistics**: Regularly check statistics to tune limits
5. **Set Appropriate Limits**: Balance security with performance requirements
6. **Use Multi-Level Limiting**: Combine global, IP, and user limits for best protection
7. **Log Rejections**: Log rate limit violations for monitoring and debugging

## Security Considerations

- The module limits DoS attacks by controlling connection and request rates
- IP-based limits prevent single-source flooding
- User-based limits prevent credential abuse
- Statistics tracking helps detect attack patterns
- Thread-safe implementation allows concurrent access

## Future Enhancements

- Database-level rate limiting
- Per-IP whitelist support
- Rate limit violation alerts
- Dynamic limit adjustment based on system load
- Integration with existing TDengine monitoring system
