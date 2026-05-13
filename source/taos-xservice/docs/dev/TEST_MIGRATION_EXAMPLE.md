# 集成测试重构 - 示例迁移

本文档展示如何将现有的 Kafka 测试迁移到新的测试架构中。

## 目录结构对比

### Before (旧结构)
```
tests/
├── kafka/
│   ├── basic_test.rs
│   └── advanced_test.rs
└── e2e/
    └── test_function/
        └── kafka_test.py
```

### After (新结构)
```
tests/
└── integration/
    ├── datasources/
    │   └── kafka/
    │       ├── mod.rs
    │       ├── basic.rs
    │       └── advanced.rs
    └── e2e/
        └── scenarios/
            └── kafka/
                ├── test_basic.py
                └── test_advanced.py
```

## 示例 1: 迁移 Rust 集成测试

### 旧代码 (tests/kafka/basic_test.rs)

```rust
use tokio;
use taosx_task::kafka_to_taos;
use taos::Dsn;

#[tokio::test]
async fn test_kafka_basic_connection() {
    let kafka_dsn = "kafka://localhost:9092/test".parse().unwrap();
    let taos_dsn = "taos://localhost:6030/test".parse().unwrap();
    
    // Test logic here
    let result = kafka_to_taos(
        kafka_dsn,
        None,
        taos_dsn,
        CancellationToken::new(),
        None,
        None,
        flume::unbounded().0,
    ).await;
    
    assert!(result.is_ok());
}
```

### 新代码 (tests/integration/datasources/kafka/basic.rs)

```rust
//! Kafka datasource integration tests
//! 
//! These tests require:
//! - Kafka broker running on localhost:9092
//! - TDengine server running on localhost:6030
//! 
//! Run with: cargo make test-datasource-kafka

use tokio;
use taosx_task::kafka_to_taos;
use taos::Dsn;
use tokio_util::sync::CancellationToken;

use crate::common::{
    setup_kafka_topic,
    cleanup_kafka_topic,
    setup_taos_database,
    cleanup_taos_database,
};

#[cfg(feature = "test-kafka")]
mod basic_tests {
    use super::*;

    /// Test basic Kafka to TDengine connection
    /// 
    /// Scenario:
    /// 1. Create a Kafka topic
    /// 2. Produce sample messages
    /// 3. Start kafka_to_taos task
    /// 4. Verify data in TDengine
    #[tokio::test]
    async fn test_kafka_basic_connection() {
        // Setup
        let test_id = uuid::Uuid::new_v4();
        let topic = format!("test-kafka-basic-{}", test_id);
        let db = format!("test_kafka_basic_{}", test_id.as_simple());
        
        setup_kafka_topic(&topic).await.expect("Failed to create topic");
        setup_taos_database(&db).await.expect("Failed to create database");
        
        // Test
        let kafka_dsn = format!("kafka://localhost:9092/{}?group.id=test", topic)
            .parse()
            .unwrap();
        let taos_dsn = format!("taos://localhost:6030/{}", db)
            .parse()
            .unwrap();
        
        let result = kafka_to_taos(
            kafka_dsn,
            None,
            taos_dsn,
            CancellationToken::new(),
            None,
            None,
            flume::unbounded().0,
        ).await;
        
        // Verify
        assert!(result.is_ok(), "kafka_to_taos failed: {:?}", result.err());
        
        // Cleanup
        cleanup_kafka_topic(&topic).await.ok();
        cleanup_taos_database(&db).await.ok();
    }

    /// Test Kafka connection with invalid broker
    /// 
    /// Should fail gracefully with proper error message
    #[tokio::test]
    async fn test_kafka_invalid_broker() {
        let kafka_dsn = "kafka://invalid-host:9999/test".parse().unwrap();
        let taos_dsn = "taos://localhost:6030/test".parse().unwrap();
        
        let result = kafka_to_taos(
            kafka_dsn,
            None,
            taos_dsn,
            CancellationToken::new(),
            None,
            None,
            flume::unbounded().0,
        ).await;
        
        assert!(result.is_err(), "Expected error for invalid broker");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("broker"),
            "Error should mention broker: {}",
            err
        );
    }
    
    /// Test Kafka with authentication
    #[tokio::test]
    #[cfg(feature = "test-kafka-auth")]
    async fn test_kafka_with_sasl() {
        // Test SASL authentication
        let test_id = uuid::Uuid::new_v4();
        let topic = format!("test-kafka-auth-{}", test_id);
        
        let kafka_dsn = format!(
            "kafka://localhost:9092/{}?\
            security.protocol=SASL_PLAINTEXT&\
            sasl.mechanism=PLAIN&\
            sasl.username=admin&\
            sasl.password=admin-secret",
            topic
        ).parse().unwrap();
        
        let taos_dsn = "taos://localhost:6030/test".parse().unwrap();
        
        let result = kafka_to_taos(
            kafka_dsn,
            None,
            taos_dsn,
            CancellationToken::new(),
            None,
            None,
            flume::unbounded().0,
        ).await;
        
        assert!(result.is_ok());
    }
}
```

### 新代码 (tests/integration/datasources/kafka/mod.rs)

```rust
//! Kafka integration tests module
//! 
//! Test structure:
//! - basic.rs: Basic connection and simple scenarios
//! - advanced.rs: Complex scenarios, transformations
//! - performance.rs: Performance benchmarks

mod basic;
mod advanced;

#[cfg(feature = "test-kafka-perf")]
mod performance;

// Re-export common test utilities
pub use crate::common::kafka::*;
```

## 示例 2: 迁移 Python E2E 测试

### 旧代码 (tests/e2e/test_function/kafka_test.py)

```python
import pytest
import logging

kafka_test_logger = logging.getLogger(__name__)

def test_kafka_basic():
    """Test basic Kafka functionality"""
    # Test code here
    pass

def test_kafka_with_transform():
    """Test Kafka with data transformation"""
    # Test code here
    pass
```

### 新代码 (tests/integration/e2e/scenarios/kafka/test_basic.py)

```python
"""
Kafka Basic E2E Tests

Test scenarios for basic Kafka integration functionality.
These tests cover end-to-end scenarios from Kafka to TDengine.

Requirements:
- Kafka broker running and accessible
- TDengine server running
- taosX service running

Run with:
    poetry run pytest -m kafka
    poetry run pytest scenarios/kafka/test_basic.py
"""
import pytest
import logging
import time
from typing import Dict, Any

from testng_taosx.constant import TaskType
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter, Util
from testng_taosx.kafkaPub import Producer

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def kafka_config() -> Dict[str, Any]:
    """
    Load Kafka test configuration
    
    Returns:
        Dict containing Kafka broker info, test topics, etc.
    """
    env_data = Util.get_env_data()
    return {
        "broker": env_data.get("kafka_broker", "localhost:9092"),
        "topic_prefix": "taosx_test",
        "group_prefix": "taosx_group",
    }


@pytest.fixture(scope="function")
def test_context(kafka_config) -> Dict[str, Any]:
    """
    Create isolated test context for each test
    
    Generates unique topic and database names to avoid conflicts.
    """
    import uuid
    test_id = str(uuid.uuid4())[:8]
    
    context = {
        "test_id": test_id,
        "topic": f"{kafka_config['topic_prefix']}_{test_id}",
        "group": f"{kafka_config['group_prefix']}_{test_id}",
        "database": f"test_kafka_{test_id}",
    }
    
    logger.info(f"Test context created: {context}")
    
    yield context
    
    # Cleanup
    logger.info(f"Cleaning up test context: {context['test_id']}")
    # Add cleanup logic here


@pytest.mark.kafka
@pytest.mark.sanity
def test_kafka_basic_produce_consume(test_context):
    """
    Test Case: Basic Kafka produce and consume
    
    Scenario:
    1. Create a Kafka topic
    2. Produce JSON messages to the topic
    3. Create taosX task to consume messages
    4. Verify data is written to TDengine
    
    Expected:
    - All messages consumed successfully
    - Data matches source format
    - No data loss
    """
    logger.info("=== Starting test_kafka_basic_produce_consume ===")
    
    # Step 1: Prepare test data
    test_messages = [
        {
            "timestamp": int(time.time() * 1000),
            "device_id": "sensor_001",
            "temperature": 25.5,
            "humidity": 60.2,
        }
        for i in range(100)
    ]
    
    # Step 2: Produce messages to Kafka
    producer = Producer(test_context["topic"])
    for msg in test_messages:
        producer.send_json(msg)
    producer.flush()
    
    # Step 3: Create taosX task
    task_config = {
        "name": f"kafka_basic_test_{test_context['test_id']}",
        "from": (
            f"kafka://localhost:9092/{test_context['topic']}"
            f"?group.id={test_context['group']}"
            f"&auto.offset.reset=earliest"
        ),
        "to": f"taos://localhost:6030/{test_context['database']}",
        "parser": {
            "type": "json",
            "timestamp": "timestamp",
            "tags": ["device_id"],
            "fields": ["temperature", "humidity"],
        }
    }
    
    task = Task(task_config)
    task.start()
    
    # Step 4: Wait for data ingestion
    time.sleep(5)
    
    # Step 5: Verify data in TDengine
    taos = TaosAdapter(test_context["database"])
    
    count = taos.query_scalar(
        "SELECT COUNT(*) FROM meters"
    )
    assert count == len(test_messages), \
        f"Expected {len(test_messages)} rows, got {count}"
    
    # Verify data content
    first_row = taos.query_one(
        "SELECT * FROM meters ORDER BY ts LIMIT 1"
    )
    assert first_row["device_id"] == "sensor_001"
    assert abs(first_row["temperature"] - 25.5) < 0.01
    
    # Cleanup
    task.stop()
    taos.close()
    
    logger.info("=== test_kafka_basic_produce_consume PASSED ===")


@pytest.mark.kafka
@pytest.mark.sanity
def test_kafka_with_agent(test_context):
    """
    Test Case: Kafka integration with taosX agent
    
    Scenario:
    1. Start taosX agent
    2. Create task to run on agent
    3. Verify data flow through agent
    
    This tests the agent architecture for distributed deployments.
    """
    pytest.skip("Agent tests require separate agent setup")
    # Implementation here


@pytest.mark.kafka
@pytest.mark.performance
@pytest.mark.slow
def test_kafka_high_throughput(test_context):
    """
    Test Case: High throughput message processing
    
    Scenario:
    1. Produce 1 million messages to Kafka
    2. Measure ingestion rate to TDengine
    3. Verify no data loss
    
    Performance target:
    - Throughput: > 50k msgs/sec
    - Latency: < 100ms p99
    - Data loss: 0%
    """
    pytest.skip("Performance tests run separately")
    # Implementation here


@pytest.mark.kafka
@pytest.mark.sanity
def test_kafka_error_handling_invalid_json(test_context):
    """
    Test Case: Error handling for invalid JSON messages
    
    Scenario:
    1. Produce mix of valid and invalid JSON
    2. Verify task continues processing valid messages
    3. Check error logs for invalid messages
    
    Expected:
    - Valid messages processed successfully
    - Invalid messages logged but not crash
    - Error metrics updated
    """
    # Implementation here
    pass


@pytest.mark.kafka
@pytest.mark.sanity
def test_kafka_schema_evolution(test_context):
    """
    Test Case: Handle schema changes in messages
    
    Scenario:
    1. Produce messages with schema v1
    2. Switch to schema v2 (added field)
    3. Verify both schemas handled correctly
    
    This tests backward/forward compatibility.
    """
    # Implementation here
    pass
```

### 新代码 (tests/integration/e2e/scenarios/kafka/conftest.py)

```python
"""
Kafka test fixtures and configuration

Shared fixtures for Kafka E2E tests.
"""
import pytest
import logging
from typing import Generator

from testng_taosx.kafkaPub import KafkaAdmin

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def kafka_admin() -> Generator[KafkaAdmin, None, None]:
    """
    Kafka admin client for test setup/teardown
    """
    admin = KafkaAdmin()
    yield admin
    admin.close()


@pytest.fixture(autouse=True)
def check_kafka_availability():
    """
    Check if Kafka is available before running tests
    
    Skip tests if Kafka is not accessible.
    """
    import socket
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        
        if result != 0:
            pytest.skip("Kafka broker not available on localhost:9092")
    except Exception as e:
        pytest.skip(f"Failed to check Kafka availability: {e}")
```

## 示例 3: 添加通用测试工具

### tests/integration/common/fixtures.rs

```rust
//! Common test fixtures and utilities

use std::sync::Arc;
use uuid::Uuid;
use anyhow::Result;

/// Generate unique test identifier
pub fn test_id() -> String {
    Uuid::new_v4().as_simple().to_string()
}

/// Common Kafka test utilities
pub mod kafka {
    use super::*;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
    use rdkafka::ClientConfig;
    
    /// Create a Kafka topic for testing
    pub async fn setup_kafka_topic(topic: &str) -> Result<()> {
        let admin: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .create()?;
        
        let new_topic = NewTopic::new(topic, 1, rdkafka::admin::TopicReplication::Fixed(1));
        admin
            .create_topics(&[new_topic], &AdminOptions::new())
            .await?;
        
        Ok(())
    }
    
    /// Delete a Kafka topic
    pub async fn cleanup_kafka_topic(topic: &str) -> Result<()> {
        let admin: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .create()?;
        
        admin
            .delete_topics(&[topic], &AdminOptions::new())
            .await?;
        
        Ok(())
    }
}

/// Common TDengine test utilities
pub mod taos {
    use super::*;
    use taos::*;
    
    /// Create a test database
    pub async fn setup_taos_database(db: &str) -> Result<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;
        
        taos.exec(format!("CREATE DATABASE IF NOT EXISTS {}", db))
            .await?;
        
        Ok(())
    }
    
    /// Drop a test database
    pub async fn cleanup_taos_database(db: &str) -> Result<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;
        
        taos.exec(format!("DROP DATABASE IF EXISTS {}", db))
            .await?;
        
        Ok(())
    }
}
```

## 更新 Cargo.toml

### tests/Cargo.toml

```toml
[package]
name = "taosx-integration-tests"
version = "0.1.0"
edition = "2021"
publish = false

[features]
default = []

# Core features
integration = []

# Data source tests
test-kafka = []
test-mysql = []
test-oracle = []
test-postgres = []

# Special configurations
test-kafka-auth = ["test-kafka"]
test-kafka-perf = ["test-kafka"]

# Test groups
test-databases = ["test-mysql", "test-oracle", "test-postgres"]
test-all = ["test-databases", "test-kafka"]

[dependencies]
# Test framework
tokio = { workspace = true, features = ["full", "test-util"] }
tokio-util = { workspace = true }

# taosX dependencies
taosx-core = { path = "../../taosx-core" }
taosx-task = { path = "../../crates/task" }
taos = { workspace = true }

# Kafka testing
rdkafka = { version = "0.36", optional = true, features = ["cmake-build"] }

# Utilities
uuid = { version = "1.0", features = ["v4"] }
anyhow = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }

[[test]]
name = "integration_tests"
path = "integration/lib.rs"
```

## 更新 Makefile.toml

在 `Makefile.toml` 中添加：

```toml
# Kafka specific tests
[tasks.test-datasource-kafka]
description = "Run Kafka integration tests"
env = { "RUST_LOG" = "debug" }
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-kafka",
    "-E", "test(/kafka/)",
]

[tasks.test-datasource-kafka-perf]
description = "Run Kafka performance tests"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-kafka-perf",
    "-E", "test(/kafka.*perf/)",
]

# Python E2E tests
[tasks.e2e-kafka]
description = "Run Kafka E2E tests"
script = """
cd tests/integration/e2e
poetry run pytest -m kafka -v --tb=short
"""
```

## 验证迁移

```bash
# 1. 运行 Rust 测试
cargo make test-datasource-kafka

# 2. 运行 Python 测试
cargo make e2e-kafka

# 3. 运行完整测试套件
cargo make test-quick

# 4. 查看测试列表
cargo nextest list -p taosx-integration-tests --features test-kafka
```

## 总结

这个示例展示了：

1. ✅ 如何重组测试文件结构
2. ✅ 如何添加合适的 feature 标志
3. ✅ 如何使用测试标签 (pytest.mark)
4. ✅ 如何创建共享的测试工具
5. ✅ 如何更新 cargo make 任务
6. ✅ 如何编写清晰的测试文档

迁移其他数据源的测试时，可以参考这个模式进行类似的调整。

---

**下一步**: 参考 [TEST_REFACTORING_PLAN.md](TEST_REFACTORING_PLAN.md) 了解完整的重构计划
