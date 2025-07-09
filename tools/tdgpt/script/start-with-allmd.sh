#!/bin/bash

start_service() {
  echo "Starting $1 service"
  $1
}

start_service "systemctl start taosanoded"
start_service "start-tdtsfm"
start_service "start-timer-moe"
