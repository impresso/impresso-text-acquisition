#!/usr/bin/env bash
# Mirror the BnF OLR SFTP deposit (/deposit/olr) to the local raw data folder.
# Idempotent/resumable: safe to re-run after an interruption (lftp mirror -c
# resumes, and skips files whose size already matches the remote side).
#
# Usage:
#   export BNF_SFTP_PASSWORD='...'
#   ./fetch_bnf_olr.sh

set -euo pipefail

: "${BNF_SFTP_PASSWORD:?Set BNF_SFTP_PASSWORD before running this script}"

REMOTE_HOST="pef.bnf.fr"
REMOTE_PORT=2222
REMOTE_USER="IMPRESSO"
REMOTE_DIR="/deposit/olr"
LOCAL_DIR="/mnt/project_impresso_rw/original/BNF/new_olr"
LOG_FILE="${LOCAL_DIR}/_fetch_bnf_olr.log"

mkdir -p "$LOCAL_DIR"

lftp -u "${REMOTE_USER},${BNF_SFTP_PASSWORD}" -p "$REMOTE_PORT" "sftp://${REMOTE_HOST}" -e "
  set sftp:auto-confirm yes;
  set net:timeout 30;
  set net:max-retries 5;
  set net:reconnect-interval-base 5;
  set mirror:parallel-transfer-count 6;
  mirror --continue --parallel=6 --verbose=3 --log=${LOG_FILE} ${REMOTE_DIR} ${LOCAL_DIR};
  bye
"
