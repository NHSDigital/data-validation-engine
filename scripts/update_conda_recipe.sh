#!/bin/bash

RECIPE_PATH=$(dirname $0)/../conda_recipe/meta.yaml

#old values
if [[ -n "${CZ_PRE_CURRENT_VERSION}" ]]
    then
        OLD_VERSION=$CZ_PRE_CURRENT_VERSION
    else
        OLD_VERSION=$(cat $RECIPE_PATH | grep -E "^  version: [0-9]+\.[0-9]+\.[0-9]+$" | sed -E s"/version:| //g")
fi
    
OLD_SHA=$(cat $RECIPE_PATH | grep -E "^  sha256: [0-9a-z]+" | sed -E "s/sha256:| //g")
echo "DVE old version: $OLD_VERSION"
echo "DVE old SHA: $OLD_SHA"

#new values
if [[ -n $CZ_PRE_NEW_VERSION ]]
    then
        VERSION=$CZ_PRE_NEW_VERSION
    else
        VERSION=$(cat $(dirname $0)/../pyproject.toml | grep -E "^version = \"[0-9]+\.[0-9]+\.[0-9]+\"$" | sed -E "s/version =| |\"//g")
fi

echo "DVE new version: $VERSION"
echo "Calculating sha256 checksum of new dve release ${VERSION} tarball"
make dist
RELEASE_SHA=$(sha256sum "./dist/data_validation_engine-${VERSION}.tar.gz" | cut -d ' ' -f1)
echo "DVE new SHA: $RELEASE_SHA"
rm -rf ./dist

sed -i -e "s/${OLD_VERSION}/${VERSION}/g" -e "s/${OLD_SHA}/${RELEASE_SHA}/g" $RECIPE_PATH
git add $RECIPE_PATH
