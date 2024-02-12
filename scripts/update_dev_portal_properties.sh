#!/usr/bin/env bash

set -e

if [ -z "$KBC_COMPONENT" ]; then
    echo "Error: KBC_COMPONENT environment variable is not set."
    exit 1
fi

echo "Component names to update: KBC_COMPONENT"

# Split the string into an array using comma as delimiter
IFS=',' read -r -a COMPONENTS <<< "KBC_COMPONENT"

# Obtain the component repository and log in
docker pull quay.io/keboola/developer-portal-cli-v2:latest

update_property() {
    local component_name="$1"
    local prop_name="$2"
    local file_path="$3"
    local value=$(<"$file_path")
    local app_name="kds-team.${component_name}"
    echo "Updating $prop_name for $app_name"
    echo "$value"
    if [ ! -z "$value" ]; then
        docker run --rm \
            -e KBC_DEVELOPERPORTAL_USERNAME \
            -e KBC_DEVELOPERPORTAL_PASSWORD \
            quay.io/keboola/developer-portal-cli-v2:latest \
            update-app-property ${KBC_DEVELOPERPORTAL_VENDOR} "$app_name" "$prop_name" --value="$value"
        echo "Property $prop_name updated successfully for $app_name"
    else
        echo "$prop_name is empty for $app_name!"
        exit 1
    fi
}

for component in "${COMPONENTS[@]}"; do
    echo "Updating properties for component: $component"
    update_property "$component" "longDescription" "db_components/$component/component_config/component_long_description.md"
    update_property "$component" "configurationSchema" "db_components/$component/component_config/configSchema.json"
    update_property "$component" "configurationRowSchema" "db_components/$component/component_config/configRowSchema.json"
    update_property "$component" "configurationDescription" "db_components/$component/component_config/configuration_description.md"
    update_property "$component" "shortDescription" "db_components/$component/component_config/component_short_description.md"
    update_property "$component" "logger" "db_components/$component/component_config/logger"
    update_property "$component" "loggerConfiguration" "db_components/$component/component_config/loggerConfiguration.json"
done
