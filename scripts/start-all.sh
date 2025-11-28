#!/bin/bash

echo "--- Primer Ciclo: Inicialización y Parada Forzada ---"

nohup dramax worker --processes 1 &

sleep 10

echo "Terminando procesos dramax con killall..."
# TODO: Solve error, worker not working the first time
killall dramax

nohup dramax worker --processes 1 &

exec dramax server