#!/usr/bin/env bash

set -e

if [ -z "$APP_NAME" ]; then
    echo "Error: APP_NAME environment variable is not set."
    exit 1
fi

if [ -z "$APP_ID" ]; then
    echo "Error: APP_ID environment variable is not set."
    exit 1
fi

component_path=$(echo "$APP_NAME" | tr '-' '_')

# Obtain the component repository and log in
docker pull quay.io/keboola/developer-portal-cli-v2:latest

echo "Component name to update: $APP_ID"
echo "Component path: $component_path"
echo "KBC_DEVELOPERPORTAL_VENDOR: $KBC_DEVELOPERPORTAL_VENDOR"


update_property() {
    local component_name="$1"
    local prop_name="$2"
    local file_path="$3"
    local value=$(<"$file_path")
    local app_id="${KBC_DEVELOPERPORTAL_VENDOR}.${component_name}"
    echo "Updating $prop_name for $app_id"
    echo "$value"
    if [ ! -z "$value" ]; then
        docker run --rm \
            -e KBC_DEVELOPERPORTAL_USERNAME \
            -e KBC_DEVELOPERPORTAL_PASSWORD \
            quay.io/keboola/developer-portal-cli-v2:latest \
            update-app-property "$KBC_DEVELOPERPORTAL_VENDOR" "$app_id" "$prop_name" --value="$value"
        echo "Property $prop_name updated successfully for $app_id"
    else
        echo "$prop_name is empty for $app_id!"
        exit 1
    fi
}


echo "Updating properties for component: $APP_ID"
update_property "$APP_ID" "longDescription" "db_components/$component_path/component_config/component_long_description.md"
update_property "$APP_ID" "configurationSchema" "db_components/$component_path/component_config/configSchema.json"
update_property "$APP_ID" "configurationRowSchema" "db_components/$component_path/component_config/configRowSchema.json"
update_property "$APP_ID" "configurationDescription" "db_components/$component_path/component_config/configuration_description.md"
update_property "$APP_ID" "shortDescription" "db_components/$component_path/component_config/component_short_description.md"
update_property "$APP_ID" "logger" "db_components/$component_path/component_config/logger"
update_property "$APP_ID" "loggerConfiguration" "db_components/$component_path/component_config/loggerConfiguration.json"

