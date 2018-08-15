#!/usr/bin/env bash

./job.start.dsv01.download.sh

./job.start.dsv05.download.sh

./job.start.dsv01.elastic.consumer.sh
./job.start.dsv05.elastic.consumer.sh