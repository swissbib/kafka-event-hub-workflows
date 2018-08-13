#!/usr/bin/env bash

setsid python dsv05_kafka_to_elastic.py > logs/stdout.log 2>&1 < /dev/null &