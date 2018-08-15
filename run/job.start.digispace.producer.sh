#!/usr/bin/env bash

ssh -f -o ExitOnForwardFailure=yes -L 9200:localhost:9200 waeber@ub-afrikaportal.ub.unibas.ch sleep 10

setsid python run.py digispace-producer > logs/stdout.log 2>&1 < /dev/null &