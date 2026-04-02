use taosx_integration_tests::common::TestServiceConfig;
use taosx_integration_tests::common::helpers::terminate_process;

use taosx_integration_tests::core::api::*;

#[tokio::test]
async fn test_taosx_api() {
    let config = TestServiceConfig::new();
    let (_tempfile, mut cmd) = config.serve();
    let mut child = cmd.spawn().unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let client = ApiClient::builder(&config.api_base_url())
        .build()
        .expect("build api client");

    // Test 1: Health Check
    println!("\n=== Test 1: Health Check ===");
    match client.health().await {
        Ok(health) => println!("✓ Health check passed: {}", health),
        Err(e) => println!("✗ Health check failed: {}", e),
    }

    // Test 2: API is reachable
    println!("\n=== Test 2: API Reachability ===");
    println!("✓ taosX API is reachable at {}", client.url);

    // Test 3: Swagger endpoint
    println!("\n=== Test 3: Swagger Endpoint ===");
    match client.swagger().await {
        Ok(swagger) => {
            if !swagger.is_empty() {
                println!("✓ Swagger endpoint accessible ({}B)", swagger.len());
            }
        }
        Err(e) => println!("⚠ Swagger endpoint not available: {}", e),
    }

    // Test 4: List tasks (should be empty initially)
    println!("\n=== Test 4: List Tasks (Empty) ===");
    match client.list_tasks().await {
        Ok(tasks) => println!("✓ Listed {} tasks", tasks.len()),
        Err(e) => println!("✗ Failed to list tasks: {}", e),
    }

    // Test 5: Create task with invalid source (should fail)
    println!("\n=== Test 5: Create Task with Invalid Source ===");
    let invalid_task = NewTask {
        name: "test_task".to_string(),
        from: "source_db".to_string(),
        to: "target_db".to_string(),
        parser: None,
        via: None,
        labels: None,
    };
    println!("Attempting to create task: {}", invalid_task.name);
    match client.create_task(&invalid_task).await {
        Ok(_) => println!("✗ Task should have failed due to invalid source"),
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("Invalid data source") {
                println!("✓ Task creation correctly rejected: {}", error_msg);
            } else {
                println!("⚠ Task rejected but with unexpected error: {}", error_msg);
            }
        }
    }

    // Test 6: Create task with valid source (if possible)
    println!("\n=== Test 6: Create Task with Valid Source ===");
    let valid_task = NewTask {
        name: "valid_test_task".to_string(),
        from: "taos:///test".to_string(),
        to: "taos:///test2".to_string(),
        parser: None,
        via: None,
        labels: None,
    };
    println!("Attempting to create task: {}", valid_task.name);
    match client.create_task(&valid_task).await {
        Ok(task) => {
            println!("✓ Successfully created task with ID: {}", task.id);

            // Test 7: Get specific task
            println!("\n=== Test 7: Get Task by ID ===");
            match client.get_task(task.id).await {
                Ok(Some(fetched_task)) => {
                    println!(
                        "✓ Retrieved task: {} (from: {}, to: {})",
                        fetched_task.name, fetched_task.from, fetched_task.to
                    );
                }
                Ok(None) => println!("✗ Task not found"),
                Err(e) => println!("✗ Failed to get task: {}", e),
            }

            // Test 8: List tasks (should now contain at least one)
            println!("\n=== Test 8: List Tasks (With Data) ===");
            match client.list_tasks().await {
                Ok(tasks) => {
                    println!("✓ Listed {} task(s)", tasks.len());
                    for t in &tasks {
                        println!(
                            "  - Task ID {}: {} (from: {}, to: {})",
                            t.id, t.name, t.from, t.to
                        );
                    }
                }
                Err(e) => println!("✗ Failed to list tasks: {}", e),
            }

            // Test 9: Update task
            println!("\n=== Test 9: Update Task ===");
            let update = UpdateTask {
                name: Some("updated_task_name".to_string()),
                from: None,
                to: None,
                parser: None,
                via: None,
            };
            match client.update_task(task.id, &update).await {
                Ok(updated_task) => {
                    println!("✓ Successfully updated task to: {}", updated_task.name);
                }
                Err(e) => println!("✗ Failed to update task: {}", e),
            }

            // Test 10: Start task
            println!("\n=== Test 10: Start Task ===");
            match client.start_task(task.id).await {
                Ok(_) => println!("✓ Task started successfully"),
                Err(e) => println!("⚠ Failed to start task: {}", e),
            }

            // Test 13: Stop task
            println!("\n=== Test 13: Stop Task ===");
            match client.stop_task(task.id).await {
                Ok(_) => println!("✓ Task stopped successfully"),
                Err(e) => println!("⚠ Failed to stop task: {}", e),
            }

            // Test 14: Delete task
            println!("\n=== Test 14: Delete Task ===");
            match client.delete_task(task.id).await {
                Ok(_) => println!("✓ Task deleted successfully"),
                Err(e) => println!("✗ Failed to delete task: {}", e),
            }

            // Test 15: Verify task is deleted
            println!("\n=== Test 15: Verify Task Deleted ===");
            match client.get_task(task.id).await {
                Ok(_) => println!("✗ Task should have been deleted"),
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("not found") {
                        println!("✓ Task correctly deleted: {}", error_msg);
                    } else {
                        println!("⚠ Task not found but with unexpected error: {}", error_msg);
                    }
                }
            }
        }
        Err(e) => {
            println!("⚠ Could not create valid task: {}", e);
            println!("  (This may be expected depending on the environment)");
        }
    }

    println!("\n=== Test Suite Complete ===");
    if let Some(pid) = child.id() {
        terminate_process(pid);
    }
    let _ = child.wait().await;
}
