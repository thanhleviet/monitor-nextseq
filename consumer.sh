#!/usr/bin/env bash
. venv/bin/activate
huey_consumer.py main.huey -w 2 -f
