from taostest import run_case

if __name__ == '__main__':
    run_case("cloud_test.yaml", "cloud/connector/rust.py")