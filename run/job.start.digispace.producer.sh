#!/usr/bin/env bash

setsid python run.py digispace > logs/stdout.log 2>&1 < /dev/null &