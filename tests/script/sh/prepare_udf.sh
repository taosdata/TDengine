#!/bin/sh

echo -n "hello" > /tmp/normal
echo -n "" > /tmp/empty
dd if=/dev/zero bs=3584 of=/tmp/big count=1

