#!/bin/bash
docker build . -t ghcr.io/teammistake/dummy:latest
docker push ghcr.io/teammistake/dummy:latest
