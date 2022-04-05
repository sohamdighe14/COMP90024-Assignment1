#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task 1
#SBATCH --output=./outputs/n1c1-%x.%j.out
#SBATCH --error=./outputs/n1c1-%x.%j.err
module purge
module load python/3.8.6
module load mpi4py/3.0.2-timed-pingpong
time mpirun python ./main.py --dataset bigTwitter.json --langcodes language_codes.json
