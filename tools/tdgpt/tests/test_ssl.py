#!/usr/bin/env python
# encoding: utf-8
"""
Test script for tdgpt SSL/HTTPS connectivity
"""
import argparse
import sys
import urllib3

try:
    import requests
except ImportError:
    print("Error: requests library is required")
    print("Install it with: pip install requests")
    sys.exit(1)

# Suppress SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def test_https_connection(url, ca_bundle=None, insecure=False):
    """
    Test HTTPS connection to the tdgpt service

    :param url: HTTPS URL to test (e.g., https://localhost:6035)
    :param ca_bundle: Path to CA certificate bundle (optional)
    :param insecure: Skip certificate verification (for self-signed certs)
    :return: True if connection successful, False otherwise
    """
    endpoints = [
        '/status',
        '/list',
        '/models',
    ]

    print(f"Testing HTTPS connection to {url}")
    print(f"Certificate verification: {'disabled' if insecure else 'enabled'}")
    if ca_bundle:
        print(f"CA bundle: {ca_bundle}")
    print("")

    verify = False if insecure else (ca_bundle or True)
    success = True

    for endpoint in endpoints:
        test_url = url + endpoint
        try:
            response = requests.get(test_url, verify=verify, timeout=5)
            status_code = response.status_code
            result = "✓" if response.ok else "✗"
            print(f"{result} {endpoint:30s} [{status_code}]")
            if not response.ok:
                success = False
        except requests.exceptions.ConnectionError as e:
            print(f"✗ {endpoint:30s} [ConnectionError]")
            print(f"  Error: {e}")
            success = False
        except requests.exceptions.Timeout:
            print(f"✗ {endpoint:30s} [Timeout]")
            success = False
        except requests.exceptions.RequestException as e:
            print(f"✗ {endpoint:30s} [RequestException]")
            print(f"  Error: {e}")
            success = False

    print("")
    if success:
        print("✓ All tests passed!")
        return True
    else:
        print("✗ Some tests failed!")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Test tdgpt HTTPS connectivity'
    )
    parser.add_argument(
        '-u', '--url',
        default='https://localhost:6035',
        help='HTTPS URL to test (default: https://localhost:6035)'
    )
    parser.add_argument(
        '-c', '--ca-bundle',
        help='Path to CA certificate bundle for verification'
    )
    parser.add_argument(
        '-k', '--insecure',
        action='store_true',
        help='Skip certificate verification (useful for self-signed certs)'
    )

    args = parser.parse_args()

    # Default to insecure mode for localhost (self-signed certs)
    insecure = args.insecure
    if not args.ca_bundle and 'localhost' in args.url:
        insecure = True

    success = test_https_connection(args.url, args.ca_bundle, insecure)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
