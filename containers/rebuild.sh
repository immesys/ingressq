#!/bin/bash

set -ex

pushd ../backend
go build
popd

pushd ../frontend
go build
popd

cp ../backend/backend backend/
pushd backend
docker build -t immesys/ingressq-backend .
docker push immesys/ingressq-backend
popd

cp ../frontend/frontend frontend/
pushd frontend
docker build -t immesys/ingressq-frontend .
docker push immesys/ingressq-frontend
popd
