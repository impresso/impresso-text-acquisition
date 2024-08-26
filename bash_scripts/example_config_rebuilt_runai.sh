#!/bin/bash
# script to setup environment variables and arguments to launch a rebuild
# /!\ This script should be modified and adapted to each run.

export SE_ACCESS_KEY='' # add your access key here
export SE_SECRET_KEY='' # add your secret key here

# initialize all values for launching rebuilder script
export output_bucket='' # TODO fill in
export input_bucket='' # TODO fill in

export $USER_NAME = 'piconti'

export pvc_path="/home/$USER_NAME/dhlab-data/data/$USER_NAME-data"

# log file
logfile_name="self_explanatory_logfilename.log" # TODO change
touch $pvc_path/impresso-text-acquisition/text_preparation/data/logs/rebuilt_logs/$logfile_name
export log_file="${pvc_path}/impresso-text-acquisition/text_preparation/data/logs/rebuilt_logs/${logfile_name}"

#format
export format='solr' # 'solr' or 'passim'

#output_dir
export output_dir="${pvc_path}/impresso-text-acquisition/text_preparation/data/rebuilt_out"

#filter config
filter_config_filename='chosen_or_created_config_file.json' # TODO change
export filter_config="${pvc_path}/impresso-text-acquisition/text_preparation/config/rebuilt_config/${filter_config_filename}"

#git repo
export git_repo="${pvc_path}/impresso-text-acquisition"

#temp dir
mkdir -p $pvc_path/temp_rebuilt 
export temp_dir="${pvc_path}/temp_rebuilt"

# path to the previous manifest
export prev_manifest_path=""