#!/usr/bin/env bash

set -e

if [ -z "$APP_NAME" ]; then
    echo "Error: APP_NAME environment variable is not set."
    exit 1
fi

component_path=$(echo "$APP_NAME" | tr '-' '_')

echo "Component name to update: $APP_NAME"
echo "Component path: $component_path"


# Obtain the component repository and log in
docker pull quay.io/keboola/developer-portal-cli-v2:latest

update_property() {
    local component_name="$1"
    local prop_name="$2"
    local file_path="$3"
    local value=$(<"$file_path")
    local app_name="${KBC_DEV_PORTAL_VENDOR}.${component_name}"
    echo "Updating $prop_name for $app_name"
    echo "$value"
    if [ ! -z "$value" ]; then
        docker run --rm \
            -e KBC_DEVELOPERPORTAL_USERNAME \
            -e KBC_DEVELOPERPORTAL_PASSWORD \
            quay.io/keboola/developer-portal-cli-v2:latest \
            update-app-property "$KBC_DEV_PORTAL_VENDOR" "$app_name" "$prop_name" --value="$value"
        echo "Property $prop_name updated successfully for $app_name"
    else
        echo "$prop_name is empty for $app_name!"
        exit 1
    fi
}

echo "Updating properties for component: $APP_NAME"
update_property "$APP_NAME" "longDescription" "db_components/$component_path/component_config/component_long_description.md"
update_property "$APP_NAME" "configurationSchema" "db_components/$component_path/component_config/configSchema.json"
update_property "$APP_NAME" "configurationRowSchema" "db_components/$component_path/component_config/configRowSchema.json"
update_property "$APP_NAME" "configurationDescription" "db_components/$component_path/component_config/configuration_description.md"
update_property "$APP_NAME" "shortDescription" "db_components/$component_path/component_config/component_short_description.md"
update_property "$APP_NAME" "logger" "db_components/$component_path/component_config/logger"
update_property "$APP_NAME" "loggerConfiguration" "db_components/$component_path/component_config/loggerConfiguration.json"

