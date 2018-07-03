#!/bin/bash
set -ex

rm -rf bin/
docker build --rm -t gscp:build .
docker run --rm -v $PWD/bin:/go/src/app/bin gscp:build

for f in bin/*; do gpg -ab $f; done
