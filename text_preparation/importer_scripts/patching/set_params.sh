#!/bin/bash

s3_input_bucket="canonical-data"
s3_output_bucket="canonical-staging" #"canonical-sandbox" 
canonical_repo_path="/scratch/piconti/impresso/patches_temp/impresso-text-acquisition"
#previous_manifest_path="s3://canonical-staging/canonical_v0-0-3.json"
temp_dir="/scratch/piconti/impresso/patches_temp"
log_file="/home/piconti/impresso-text-acquisition/text_importer/data/patch_logs/patch_5_rero.log"
error_log="/home/piconti/impresso-text-acquisition/text_importer/data/patch_logs/patch_5_rero_errors.log"
patch_outputs_filename="patch_5_outputs_rero.txt"