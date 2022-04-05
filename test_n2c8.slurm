#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task 1
#SBATCH --output=./C2n8-%x.%j.out
#SBATCH --error=./C2n8-%x.%j.err
module purge
module load python/3.8.6
module load mpi4py/3.0.2-timed-pingpong
time mpirun python ./main.py --dataset bigTwitter.json --langcodes language_codes.json
