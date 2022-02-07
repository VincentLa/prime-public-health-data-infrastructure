# Description

This generates the configuration necessary to move from FHIR to CDM. This directory is copied from [Microsoft](https://github.com/microsoft/FHIR-Analytics-Pipelines) FHIR to CDM Analytics Pipeline.

In generating a configuration two files must be modified `resourcesConfig.yml` and `propertiesGroupConfig.yml` by far the most helpful document in creating these yaml files is the [YAML Instructions](https://github.com/microsoft/FHIR-Analytics-Pipelines/blob/main/FhirToCdm/docs/yaml-instructions-format.md)

# Run Instructions

In order to run the CDM generator you must run 
- `npm install` within the ConfigurationGenerator directory
- `node generate_from_yaml.js -r resourcesConfig.yml -p propertiesGroupConfig.yml -o tableConfig`
- To view the schema `program.js show-schema -d tableConfig -t Patient -maxDepth 4`