#!/bin/bash
# script to setup environment variables and arguments to launch a rebuild
# /!\ This script should be modified and adapted to each run.

export SE_ACCESS_KEY='' # add your access key here
export SE_SECRET_KEY='' # add your secret key here

# initialize all values for launching rebuilder script
export output_bucket='' # TODO fill in
export input_bucket='' # TODO fill in

export $USER_NAME = 'piconti'

# old pvc where the code was before to the "data" pvc -> change to the actual pvc path of /rcp-scratch
#export pvc_path="/home/$USER_NAME/dhlab-data/data/$USER_NAME-data" 
export pcv_path="/rcp-scratch/"
export text_prep_in_pvc_path = "/${USER_NAME}/impresso/impresso-text-acquisition"
# directory with all the outputs, logs, and configs
export log_outputs_in_pvc_path = iccluster040_scratch/$USER_NAME/impresso/rebuilt_rcp

# log file goes in the dir for all rebuilt rcp outputs
logfile_name="self_explanatory_logfilename.log" # TODO change
touch $pvc_path/$log_outputs_in_pvc_path/logs/$logfile_name
export log_file="${pcv_path}/${log_outputs_in_pvc_path}/logs/${logfile_name}"

#format
export format='solr' # 'solr' or 'passim'

#output_dir
#export output_dir="${pvc_path}/impresso-text-acquisition/text_preparation/data/rebuilt_out"
export output_dir="${pcv_path}/${log_outputs_in_pvc_path}/out"

#filter config
filter_config_filename='chosen_or_created_config_file.json' # TODO change
export filter_config="${pvc_path}/$text_prep_in_pvc_path/config/${filter_config_filename}"

#git repo
export git_repo=${text_prep_in_pvc_path}

#temp dir
mkdir -p ${pvc_path}/${log_outputs_in_pvc_path}/temp
export temp_dir="${scratch_pvc_path}/temp"

# s3 path to the previous manifest
export prev_manifest_path=""