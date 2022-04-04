#!/usr/bin/env python
import argparse
import os
from datetime import datetime as dt
from mpi4py import MPI
from data_preprocessing import break_chunks, DataProcessor
from util import SydGrid,LangCodes

# Get start time for the program
STARTTIME = dt.now()
ENDTIME = None

# Start doing MPI
COMM = MPI.COMM_WORLD
SIZE = COMM.Get_size()
RANK = COMM.Get_rank()

dataset = 'smallTwitter.json'
langcodes = 'language_codes.json'
sydGrid = "sydGrid.json"

"""
argParser = argparse.ArgumentParser()
argParser.add_argument('--dataset', type = str, default = 'bigTwitter.json')
argParser.add_argument('--langcodes', type = str, default = 'language_codes.json')
args = argParser.parse_args()
"""

dataSetPath = "./" + dataset
codesPath = "./" + langcodes
gridPath = "./" + sydGrid

lc = LangCodes(codesPath)
grid = SydGrid(gridPath)

def main():
    dataProcessor = DataProcessor()
    if RANK == 0:
        dataTotSize = os.path.getsize(dataSetPath)
        sizePerProcess = dataTotSize / SIZE
        
        chunks = []
        for chunkStart, chunkSize in break_chunks(dataSetPath,
                                              int(sizePerProcess),
                                              dataTotSize):
            chunks.append({"chunkStart": chunkStart, "chunkSize": chunkSize})
    else:
        chunks = None
        
    COMM.Barrier()
    
    ## Still waiting on helper functions to be finished and defined before we can fill in blanks
    
    processorChunks = COMM.scatter(chunks, root=0)
    
    if RANK != 0:
        ENDTIME = dt.now()
        
    COMM.Barrier()
    
    ## Still waiting on helper functions to be finished and defined before we can fill in blanks
    
if __name__ == "__main__":
    main()