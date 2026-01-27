#!/bin/bash

exec dramax server &

nohup dramax worker --processes 1