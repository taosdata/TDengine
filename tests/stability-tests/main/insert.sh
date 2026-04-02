#!/bin/bash
~/.local/bin/taostest --use=common_insert.yaml --group-dir=taosc_insert --keep
~/.local/bin/taostest --use=common_insert.yaml --group-dir=restful_insert --keep
~/.local/bin/taostest --use=common_insert.yaml --group-dir=schemaless_insert --keep
