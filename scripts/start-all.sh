#!/bin/bash

nohup dramax server > dramax-server.log 2>&1 &

nohup dramax worker --processes 1 > dramax-worker.log 2>&1 &
