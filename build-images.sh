#!/usr/bin/env bash
# Costruisce le 4 immagini DENTRO Minikube. Prima esegui:  eval $(minikube docker-env)
set -e
for svc in backend worker simulator frontend; do
  docker build -f $svc/Dockerfile -t tuscany/$svc:dev .
done
