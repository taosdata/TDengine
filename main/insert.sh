#!/bin/bash
~/.local/bin/taostest --use=common_insert.yaml --concurrency=1 --group-dir=taosc_insert --keep
~/.local/bin/taostest --use=common_insert.yaml --concurrency=1 --group-dir=restful_insert --keep
~/.local/bin/taostest --use=common_insert.yaml --concurrency=1 --group-dir=schemaless_insert --keep