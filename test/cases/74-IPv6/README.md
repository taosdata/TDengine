# IPv4/IPv6 Dual Stack Network Testing

This directory contains test cases for TDengine's dual-stack IPv4/IPv6 network support.

## Test Cases

### test_ipv6_dual_stack.py

Test file for verifying IPv4/IPv6 dual-stack functionality in TDengine.

**Test Class**: `TestIPv6DualStack`

#### Test Methods

1. **test_ipv6_dual_stack_demo**
   - Summary: Test IPv4/IPv6 dual-stack network support
   - Description: Verify that TDengine supports dual-stack networking with enableIpv6=1, accepting both IPv4 and IPv6 connections
   - Since: 3.3.8.4
   - Labels: network ipv6 dual-stack
   - Jira: TD-5000

2. **test_ipv4_compatibility_mode**
   - Summary: Test IPv4-only compatibility mode
   - Description: Verify backward compatibility with IPv4-only mode when enableIpv6=0, only IPv4 connections are accepted
   - Since: 3.3.8.4
   - Labels: network ipv4 compatibility
   - Jira: TD-5001

3. **test_network_connection_modes**
   - Summary: Test different network connection modes
   - Description: Test that both dual-stack and IPv4-only modes work correctly with various connection scenarios
   - Since: 3.3.8.4
   - Labels: network modes connection
   - Jira: TD-5002

4. **test_dns_resolution_modes**
   - Summary: Test DNS resolution in different modes
   - Description: Verify DNS resolution works correctly in both dual-stack and IPv4-only modes
   - Since: 3.3.8.4
   - Labels: network dns resolution
   - Jira: TD-5003

## Running the Tests

### Prerequisites

- Python 3.8+ installed
- TDengine installed and configured
- Sufficient system resources

### Command

```bash
cd /home/yihao/3.0/TDengine/test
pytest cases/74-IPv6/test_ipv6_dual_stack.py -v
```

### Expected Results

All test cases should pass successfully, verifying:
- Dual-stack mode configuration
- IPv4/IPv6 connection support
- DNS resolution in both modes
- Backward compatibility with IPv4-only mode
- Network mode switching

## Test Coverage

The tests cover:
- ✅ Dual-stack network initialization
- ✅ IPv4-only compatibility verification
- ✅ DNS resolution in dual-stack mode
- ✅ DNS resolution in IPv4-only mode
- ✅ Configuration changes and restart
- ✅ Basic connectivity tests

## Known Issues

None at this time.

## Notes

- These tests require a running TDengine cluster
- Tests automatically restart dnodes with different configurations
- All tests use the standard TDengine testing framework utilities
- Results are logged for analysis and debugging