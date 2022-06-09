from taostest import run_case

if __name__ == '__main__':
    run_case("hivemq.yaml", "thirdparty/hivemq/hivemq.py")

# taostest --use=hivemq.yaml --case=thirdparty/hivemq/hivemq.py