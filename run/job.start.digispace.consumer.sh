#!/usr/bin/env bash

setsid python run.py digispace-consumer > logs/stdout.log 2>&1 < /dev/null &