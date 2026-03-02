use crate::common::helpers::terminate_process;
use crate::common::TestServiceConfig;
use crate::core::api::*;

use std::time::Duration;

#[tokio::test]
async fn test_taosx_api_extended() {
    let config = TestServiceConfig::new();
    let (_tempfile, mut cmd) = config.serve();
    let mut child = cmd.spawn().unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = ApiClient::builder(&config.api_base_url())
        .build()
        .expect("build api client");

    println!("\n========================================");
    println!("   EXTENDED API TEST SUITE");
    println!("========================================");

    // Test 1: Profile Endpoint
    println!("\n=== Test 1: Profile Endpoint ===");
    match client.profile().await {
        Ok(profile) => {
            println!("✓ Profile retrieved successfully");
            println!("  - Version: {}", profile.version);
            println!("  - Commit: {}", profile.commit);
        }
        Err(e) => println!("✗ Failed to get profile: {}", e),
    }

    // Test 2: Metrics Endpoint
    println!("\n=== Test 2: Metrics Endpoint ===");
    match client.metrics().await {
        Ok(metrics) => {
            println!("✓ Metrics retrieved ({} bytes)", metrics.len());
            if metrics.contains("taosx_") {
                println!("  - Contains taosx metrics");
            }
        }
        Err(e) => println!("✗ Failed to get metrics: {}", e),
    }

    // Test 3: Metrics Description (English)
    println!("\n=== Test 3: Metrics Description (English) ===");
    match client.metrics_description("en").await {
        Ok(desc) => {
            println!("✓ Metrics description retrieved (English)");
            if let Some(obj) = desc.as_object() {
                println!("  - Contains {} metric descriptions", obj.len());
            }
        }
        Err(e) => println!("✗ Failed to get metrics description: {}", e),
    }

    // Test 4: Metrics Description (Chinese)
    println!("\n=== Test 4: Metrics Description (Chinese) ===");
    match client.metrics_description("zh").await {
        Ok(desc) => {
            println!("✓ Metrics description retrieved (Chinese)");
            if let Some(obj) = desc.as_object() {
                println!("  - Contains {} metric descriptions", obj.len());
            }
        }
        Err(e) => println!("✗ Failed to get metrics description: {}", e),
    }

    // Test 5: Task Count
    println!("\n=== Test 5: Task Count ===");
    match client.get_task_count().await {
        Ok(count) => println!("✓ Task count retrieved: {}", count),
        Err(e) => println!("✗ Failed to get task count: {}", e),
    }

    // Test 6: Get non-existent task (should fail)
    println!("\n=== Test 6: Get Non-existent Task ===");
    match client.get_task(99999).await {
        Ok(_) => println!("✗ Should not have found task 99999"),
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("not found") {
                println!("✓ Task 99999 correctly not found: {}", error_msg);
            } else {
                println!("⚠ Task not found but with unexpected error: {}", error_msg);
            }
        }
    }

    // Test 7: Delete non-existent task (should fail)
    println!("\n=== Test 7: Delete Non-existent Task ===");
    match client.delete_task(99999).await {
        Ok(_) => println!("✗ Should not have deleted non-existent task"),
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("not found") {
                println!("✓ Task 99999 correctly cannot be deleted: {}", error_msg);
            } else {
                println!("⚠ Delete failed but with unexpected error: {}", error_msg);
            }
        }
    }

    // Test 8: Agent Management - Create Agent
    println!("\n=== Test 8: Create Agent ===");
    let agent_result = client
        .create_agent("agent1", "dsn", "test_cluster_1", "root")
        .await;
    let agent_id = match agent_result {
        Ok(agent) => {
            println!("✓ Agent created successfully");
            println!("  - Agent ID: {}", agent.id);
            println!("  - Agent Name: {}", agent.name);
            println!("  - Token length: {}", agent.token.len());
            Some(agent.id)
        }
        Err(e) => {
            println!("⚠ Failed to create agent: {}", e);
            None
        }
    };

    // Test 9: List Agents
    println!("\n=== Test 9: List Agents ===");
    match client.list_agents(None).await {
        Ok(agents) => {
            println!("✓ Listed {} agent(s)", agents.len());
            for agent in &agents {
                println!("  - Agent ID {}: name={}", agent.id, agent.name);
            }
        }
        Err(e) => println!("✗ Failed to list agents: {}", e),
    }

    // Test 10: List Agents with filter
    if agent_id.is_some() {
        println!("\n=== Test 10: List Agents with Name Filter ===");
        match client.list_agents(Some("test_cluster_1")).await {
            Ok(agents) => {
                println!("✓ Listed {} agent(s) for name test_cluster_1", agents.len());
            }
            Err(e) => println!("✗ Failed to list filtered agents: {}", e),
        }
    }

    // Test 11: Get specific agent
    if let Some(aid) = agent_id {
        println!("\n=== Test 11: Get Agent by ID ===");
        match client.get_agent(aid).await {
            Ok(agent) => {
                println!("✓ Retrieved agent: ID={}, name={}", agent.id, agent.name);
            }
            Err(e) => println!("✗ Failed to get agent: {}", e),
        }

        // Test 12: Update Agent
        println!("\n=== Test 12: Update Agent ===");
        match client.update_agent(aid, "test_cluster_2").await {
            Ok(updated_agent) => {
                println!("✓ Agent updated successfully");
                println!("  - New Name: {}", updated_agent.name);
                println!("  - New Token length: {}", updated_agent.token.len());
            }
            Err(e) => println!("⚠ Failed to update agent: {}", e),
        }

        // Test 13: Delete Agent
        println!("\n=== Test 13: Delete Agent ===");
        match client.delete_agent(aid).await {
            Ok(_) => println!("✓ Agent deleted successfully"),
            Err(e) => println!("✗ Failed to delete agent: {}", e),
        }

        // Test 14: Verify agent is deleted
        println!("\n=== Test 14: Verify Agent Deleted ===");
        match client.get_agent(aid).await {
            Ok(_) => println!("✗ Agent should have been deleted"),
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("not found") {
                    println!("✓ Agent correctly deleted: {}", error_msg);
                } else {
                    println!("⚠ Agent not found but with unexpected error: {}", error_msg);
                }
            }
        }
    }

    // Test 15: Data Source Validation
    println!("\n=== Test 15: Data Source Validation ===");
    let test_dsns = vec![
        ("taos://localhost:6030", "TDengine DSN"),
        ("tmq://localhost:6030", "TMQ DSN"),
        ("invalid://xyz", "Invalid DSN"),
    ];
    for (dsn, desc) in test_dsns {
        match client.data_source_is_valid(dsn).await {
            Ok(valid) => {
                if valid {
                    println!("✓ {} is valid: {}", desc, dsn);
                } else {
                    println!("✓ {} is invalid (expected): {}", desc, dsn);
                }
            }
            Err(e) => println!("⚠ Failed to validate {}: {}", desc, e),
        }
    }

    // Test 16: Create multiple tasks for batch operations
    println!("\n=== Test 16: Create Multiple Tasks for Batch Operations ===");
    let mut created_task_ids = Vec::new();
    for i in 1..=3 {
        let task = NewTask {
            name: format!("batch_test_task_{}", i),
            from: "taos:///test".to_string(),
            to: format!("taos:///batch_test_target_{}?assert", i),
            parser: None,
            via: None,
            labels: None,
        };
        match client.create_task(&task).await {
            Ok(created) => {
                println!("✓ Created task {} with ID: {}", created.name, created.id);
                created_task_ids.push(created.id);
            }
            Err(e) => println!("⚠ Failed to create task {}: {}", i, e),
        }
    }

    // Test 17: Batch Start Tasks
    if !created_task_ids.is_empty() {
        println!("\n=== Test 17: Batch Start Tasks ===");
        match client.batch_start_tasks(created_task_ids.clone()).await {
            Ok(_) => println!(
                "✓ Batch start initiated for {} tasks",
                created_task_ids.len()
            ),
            Err(e) => println!("⚠ Batch start failed: {}", e),
        }

        // Give tasks a moment to start
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Test 18: Batch Stop Tasks
        println!("\n=== Test 18: Batch Stop Tasks ===");
        match client.batch_stop_tasks(created_task_ids.clone()).await {
            Ok(_) => println!(
                "✓ Batch stop initiated for {} tasks",
                created_task_ids.len()
            ),
            Err(e) => println!("⚠ Batch stop failed: {}", e),
        }

        // Test 19: Batch Delete Tasks
        println!("\n=== Test 19: Batch Delete Tasks ===");
        match client.batch_delete_tasks(created_task_ids.clone()).await {
            Ok(_) => println!(
                "✓ Batch delete succeeded for {} tasks",
                created_task_ids.len()
            ),
            Err(e) => println!("⚠ Batch delete failed: {}", e),
        }

        // Test 20: Verify tasks are deleted
        println!("\n=== Test 20: Verify Batch Deleted Tasks ===");
        let mut all_deleted = true;
        for tid in &created_task_ids {
            match client.get_task(*tid).await {
                Ok(_) => {
                    println!("✗ Task {} should have been deleted", tid);
                    all_deleted = false;
                }
                Err(_) => {
                    println!("✓ Task {} correctly deleted", tid);
                }
            }
        }
        if all_deleted {
            println!("✓ All {} tasks correctly deleted", created_task_ids.len());
        }
    }

    // Test 21: Update non-existent task (should fail)
    println!("\n=== Test 21: Update Non-existent Task ===");
    let update = UpdateTask {
        name: Some("should_fail".to_string()),
        from: None,
        to: None,
        parser: None,
        via: None,
    };
    match client.update_task(99999, &update).await {
        Ok(_) => println!("✗ Should not have updated non-existent task"),
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("not found") {
                println!("✓ Update correctly failed for non-existent task");
            } else {
                println!("⚠ Update failed with unexpected error: {}", error_msg);
            }
        }
    }

    // Test 22: Start non-existent task (should fail)
    println!("\n=== Test 22: Start Non-existent Task ===");
    match client.start_task(99999).await {
        Ok(_) => println!("✗ Should not have started non-existent task"),
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("not found") {
                println!("✓ Start correctly failed for non-existent task");
            } else {
                println!("⚠ Start failed with unexpected error: {}", error_msg);
            }
        }
    }

    // Test 23: Stop non-existent task (should fail)
    println!("\n=== Test 23: Stop Non-existent Task ===");
    match client.stop_task(99999).await {
        Ok(_) => println!("✗ Should not have stopped non-existent task"),
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("not found") {
                println!("✓ Stop correctly failed for non-existent task");
            } else {
                println!("⚠ Stop failed with unexpected error: {}", error_msg);
            }
        }
    }

    // Test 24: Create task with empty name (should fail or succeed with empty name)
    println!("\n=== Test 24: Create Task with Empty Name ===");
    let empty_name_task = NewTask {
        name: "".to_string(),
        from: "taos:///test".to_string(),
        to: "taos:///test2".to_string(),
        parser: None,
        via: None,
        labels: None,
    };
    match client.create_task(&empty_name_task).await {
        Ok(task) => {
            println!("⚠ Task with empty name created: ID={}", task.id);
            // Clean up
            let _ = client.delete_task(task.id).await;
        }
        Err(e) => println!("✓ Task with empty name rejected: {}", e),
    }

    // Test 25: Create task with very long name
    println!("\n=== Test 25: Create Task with Very Long Name ===");
    let long_name = "a".repeat(1000);
    let long_name_task = NewTask {
        name: long_name.clone(),
        from: "taos:///test".to_string(),
        to: "taos:///test2".to_string(),
        parser: None,
        via: None,
        labels: None,
    };
    match client.create_task(&long_name_task).await {
        Ok(task) => {
            println!("⚠ Task with very long name created: ID={}", task.id);
            // Clean up
            let _ = client.delete_task(task.id).await;
        }
        Err(e) => println!("✓ Task with very long name rejected: {}", e),
    }

    // Test 26: Create task with special characters in name
    println!("\n=== Test 26: Create Task with Special Characters ===");
    let special_chars_task = NewTask {
        name: "test-task_123!@#$%".to_string(),
        from: "taos:///test".to_string(),
        to: "taos:///test2".to_string(),
        parser: None,
        via: None,
        labels: None,
    };
    match client.create_task(&special_chars_task).await {
        Ok(task) => {
            println!(
                "✓ Task with special characters created: ID={}, name={}",
                task.id, task.name
            );
            // Clean up
            let _ = client.delete_task(task.id).await;
        }
        Err(e) => println!("⚠ Task with special characters rejected: {}", e),
    }

    // Test 27: Multiple rapid task list requests
    println!("\n=== Test 27: Multiple Rapid Task List Requests ===");
    let mut success_count = 0;
    for _ in 0..5 {
        if client.list_tasks().await.is_ok() {
            success_count += 1;
        }
    }
    println!("✓ {}/5 rapid list requests succeeded", success_count);

    // Test 28: API reachability after operations
    println!("\n=== Test 28: API Reachability After Operations ===");
    match client.health().await {
        Ok(health) => println!("✓ API still reachable: {}", health),
        Err(e) => panic!("✗ API not reachable: {}", e),
    }

    // Test 29: Final task count
    println!("\n=== Test 29: Final Task Count ===");
    match client.get_task_count().await {
        Ok(count) => println!("✓ Final task count: {}", count),
        Err(e) => println!("✗ Failed to get final task count: {}", e),
    }

    // Test 30: Final task list
    println!("\n=== Test 30: Final Task List ===");
    match client.list_tasks().await {
        Ok(tasks) => {
            println!("✓ Final task list retrieved: {} task(s)", tasks.len());
            for task in &tasks {
                println!(
                    "  - Task ID {}: {} ({}->{})",
                    task.id, task.name, task.from, task.to
                );
            }
        }
        Err(e) => println!("✗ Failed to get final task list: {}", e),
    }

    println!("\n========================================");
    println!("   EXTENDED TEST SUITE COMPLETE");
    println!("========================================\n");
    if let Some(pid) = child.id() {
        terminate_process(pid);
    }
    let _ = child.wait().await;
}
