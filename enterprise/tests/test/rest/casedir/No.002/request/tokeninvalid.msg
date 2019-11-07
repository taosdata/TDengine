#!/bin/bash
root=$1
token=`bash $root/common/badtoken`
cookie="eyJ0eXBlIjoibWV0YSIsImFjY3RJZCI6IjAiLCJtZXRhZGF0YSI6WzEsMjUxNzg5MzIyLDAsMjUxOTIwMzk0LDAsMCwwLDAsMCwwLDAsMCwxMiwyLCIwLmRiMi5tZXRlcjIiXX0="
http --print HBhb POST 10.0.2.15:8080/sql/db2 Cookie:meta=$cookie sql='select * from meter2;' --auth-type=jwt --auth="$token"

