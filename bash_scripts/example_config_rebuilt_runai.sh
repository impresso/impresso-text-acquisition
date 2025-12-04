#!/bin/bash
# script to setup environment variables and arguments to launch a rebuild
# /!\ This script should be modified and adapted to each run.
echo "Setting all the configuration variables:" 

export SE_ACCESS_KEY='' # add your access key here
export SE_SECRET_KEY='' # add your secret key here

# initialize all values for launching rebuilder script
export output_bucket='' # TODO fill in
export input_bucket='' # TODO fill in

export $USER_NAME='' # TODO fill in

# old pvc where the code was before to the "data" pvc -> change to the actual pvc path of /rcp-scratch
export text_prep_in_pvc_path="/rcp-scratch/${USER_NAME}/impresso/impresso-text-acquisition"
# directory with all the outputs, logs, and configs
export log_outputs_in_pvc_path = "/rcp-scratch/${USER_NAME}/impresso/rebuilt_rcp"

# log file goes in the dir for all rebuilt rcp outputs
logfile_name="self_explanatory_logfilename.log" # TODO change
export log_file="${log_outputs_in_pvc_path}/logs/${logfile_name}"
echo "log_file=${log_file}"
touch $log_file

#format
export format='solr' # 'solr' or 'passim'
echo "format=${format}"

#output_dir
#export output_dir="${pvc_path}/impresso-text-acquisition/text_preparation/data/rebuilt_out"
export output_dir="${log_outputs_in_pvc_path}/out"
echo "output_dir=${output_dir}"

#filter config
filter_config_filename='chosen_or_created_config_file.json' # TODO change
export filter_config="$log_outputs_in_pvc_path/config/${filter_config_filename}"

#git repo
export git_repo=${text_prep_in_pvc_path}
echo "git_repo: ${git_repo}"

#temp dir
export temp_dir="${log_outputs_in_pvc_path}/temp"
echo "temp_dir: ${temp_dir}"

# s3 path to the previous manifest
export prev_manifest_path=""
echo "prev_manifest_path: ${prev_manifest_path}"