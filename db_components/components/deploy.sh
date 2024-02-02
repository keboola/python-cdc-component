#!/bin/sh
set -e

env

# compatibility with GitHub Actions
if [ ! -z "${GITHUB_REF}" ]; then
    if [ -n "${GITHUB_TAG}" ]; then
        echo "assigning GitHub tag"
        export TAG="${GITHUB_TAG}"
    elif [ -n "${GITHUB_REF}" ]; then
        # Extract branch name from GITHUB_REF
        export BRANCH_NAME=$(echo "${GITHUB_REF#refs/heads/}")
        echo "assigning GitHub branch: ${BRANCH_NAME}"
    else
        echo "No Tag/Branch is set!"
        exit 1
    fi
else
    echo "No Tag/Branch is set!"
    exit 1
fi

echo "Tag/Branch is '${TAG:-$BRANCH_NAME}'"

# check if deployment is triggered only on master
if [ "$BRANCH_NAME" != "master" ]; then
    echo "Deploy on tagged commit can only be executed on master!"
    exit 1
fi

# Obtain the component repository and log in
echo "Obtain the component repository and log in"
docker pull quay.io/keboola/developer-portal-cli-v2:latest
export REPOSITORY=$(docker run --rm \
    -e KBC_DEVELOPERPORTAL_USERNAME \
    -e KBC_DEVELOPERPORTAL_PASSWORD \
    quay.io/keboola/developer-portal-cli-v2:latest \
    ecr:get-repository ${KBC_DEVELOPERPORTAL_VENDOR} ${KBC_DEVELOPERPORTAL_APP})

echo "Set credentials"
eval $(docker run --rm \
    -e KBC_DEVELOPERPORTAL_USERNAME \
    -e KBC_DEVELOPERPORTAL_PASSWORD \
    quay.io/keboola/developer-portal-cli-v2:latest \
    ecr:get-login ${KBC_DEVELOPERPORTAL_VENDOR} ${KBC_DEVELOPERPORTAL_APP})

# Push to the repository
echo "Push to the repository"
docker tag ${APP_IMAGE}:latest ${REPOSITORY}:${TAG:-$BRANCH_NAME}
docker tag ${APP_IMAGE}:latest ${REPOSITORY}:latest
docker push ${REPOSITORY}:${TAG:-$BRANCH_NAME}
docker push ${REPOSITORY}:latest

# Update the tag in Keboola Developer Portal -> Deploy to KBC
if echo "${TAG:-$BRANCH_NAME}" | grep -c '^v\?[0-9]\+\.[0-9]\+\.[0-9]\+$'; then
    docker run --rm \
        -e KBC_DEVELOPERPORTAL_USERNAME \
        -e KBC_DEVELOPERPORTAL_PASSWORD \
        quay.io/keboola/developer-portal-cli-v2:latest \
        update-app-repository ${KBC_DEVELOPERPORTAL_VENDOR} ${KBC_DEVELOPERPORTAL_APP} ${TAG:-$BRANCH_NAME} ecr ${REPOSITORY}
else
    echo "Skipping deployment to KBC, tag/branch ${TAG:-$BRANCH_NAME} is not allowed."
fi
