#!/usr/bin/env bash

setsid python run.py dsv01-consumer > logs/stdout.log 2>&1 < /dev/null &