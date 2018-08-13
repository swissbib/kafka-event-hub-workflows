#!/usr/bin/env bash

setsid python run.py swissbib-elk > logs/stdout.log 2>&1 < /dev/null &