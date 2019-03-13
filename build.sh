#!/bin/bash
set -ex

rm -rf bin/
docker build --pull --rm -t gscp:build .
docker run --rm -v $PWD/bin:/usr/src/gscp/bin gscp:build

for f in bin/*; do gpg -ab $f; done
