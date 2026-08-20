#!/bin/bash
#SBATCH --time=7-00:00:00
#SBATCH --mem=50G
#SBATCH --cpus-per-gpu=8
#SBATCH --gpus=1
#SBATCH --gpus-per-node=1
#SBATCH --constraint=v100
#SBATCH --account=pi-violai
#SBATCH --output=/ibex/user/engeld/log/%x-%j-slurm.out
#SBATCH --exclude=

# Mirror the #SBATCH values above. ACCOUNT may be left "" if you drop
# --account entirely.
GPUTYPE=v100
ACCOUNT=pi-violai

source /ibex/user/engeld/worker-harness/slurm/wh-slurm-common.sh
wh_slurm_bootstrap "$@"

nvidia-smi
module load singularity
echo " Starting Worker Harness Container with WH_DIR=$WH_DIR"
bash /ibex/user/engeld/worker-harness/start-wh.sh
