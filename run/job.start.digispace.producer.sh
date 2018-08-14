#!/usr/bin/env bash

ssh -f -o ExitOnForwardFailure=yes -L 9200:localhost:9200 harvester@ub-afrikaportal.ub.unibas.ch:9200

sleep 10

setsid python run.py digispace > logs/stdout.log 2>&1 < /dev/null &