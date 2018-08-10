#!/usr/bin/env bash

setsid python dsv05_sru_to_kafka.py > logs/stdout.log 2>&1 < /dev/null &