#!/usr/bin/env bash

setsid python run.py dsv05-producer > logs/stdout.log 2>&1 < /dev/null &