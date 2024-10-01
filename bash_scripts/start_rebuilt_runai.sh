#!/bin/bash
# Script to be used to launch the dask local cluster and rebuilder script. 
# If using Runai, more information available https://github.com/impresso/impresso-infrastructure/blob/main/howtos/runai.md.
# Will use the environment variables in pvc (on mnt point of cdhvm0002)
# "/home/$USER_NAME/dhlab-data/data/$USER_NAME-data/config_rebuilt_runai.sh" (or other provided script) for the various configuartions necessary.

# Default number of workers
DEFAULT_WORKERS='64'
# Default config script
DEFAULT_CONFIG='config_rebuilt_runai.sh'

# Display script usage information
usage() {
  echo "Usage: $0 [-h|--help] [-w|--nworkers <num> -c|--config-script <script>]"
  echo "Options:"
  echo "  -h, --help       Display this help message"
  echo "  -w, --nworkers    Number of workers to use (default: $DEFAULT_WORKERS)"
  echo "  -c, --config-script    Config script to use (default: $DEFAULT_CONFIG)"
  exit 1
}

ARG=$1

# Parse command-line options
while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      usage
      ;;
    -w|--workers)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Error: Missing value for option -w|--workers"
        usage
      fi
      WORKERS=$1
      ;;
    -c|--config-script)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Error: Missing value for option -c|--config-script"
        usage
      fi
      CONFIG=$1
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
  shift
done

# If number of workers is not provided, use the default value
WORKERS=${WORKERS:-$DEFAULT_WORKERS}
# If config script is not provided, use the default value
CONFIG=${CONFIG:-$DEFAULT_CONFIG}

echo "Using user: $USER_NAME"
echo "Launching using configuration script $CONFIG with $WORKERS workers."

# move to directory containing init script
cd /home/$USER_NAME/dhlab-data/data/$USER_NAME-data

# make config script exectuable and execute it.
chmod -x $CONFIG
. $CONFIG

# sanity check
echo "Sanity check: env. variable log_file: $log_file, and rebuilt format: $format"

# change back to /home/$USER_NAME
cd

# locally in a screen, the following should be run:
# kubectl port-forward {job-name}-0-0 8786:8787

# launch screens
echo "Launching the scheduler, workers and rebuilder script, with $WORKERS workers."
screen -dmS scheduler dask scheduler --port 8786
screen -dmS workers dask worker localhost:8786 --nworkers $WORKERS --nthreads 1 --memory-limit 6G

echo "dask dashboard at localhost:8786/status"

screen -dmS rebuilt python $pvc_path/impresso-text-acquisition/text_preparation/rebuilders/rebuilder.py rebuild_articles --input-bucket=$input_bucket --log-file=$log_file --output-dir=$output_dir --output-bucket=$output_bucket --format=$format --filter-config=$filter_config --git-repo=$git_repo --temp-dir=$temp_dir --prev-manifest=$prev_manifest_path --scheduler=localhost:8786