#!/usr/bin/env bash

setsid python run.py enrich-user-data > logs/stdout.log 2>&1 < /dev/null &