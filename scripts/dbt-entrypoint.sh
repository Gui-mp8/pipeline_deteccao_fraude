#!/usr/bin/env sh
set -eu

dbt deps --quiet
dbt "$@" --target "${DBT_TARGET:-cloud_run}" --quiet --warn-error-options '{"error": ["NoNodesForSelectionCriteria"]}'
