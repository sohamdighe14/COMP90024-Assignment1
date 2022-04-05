#!/usr/bin/env python
import argparse
import os
from datetime import datetime as dt
from collections import Counter
from mpi4py import MPI
from data_preprocessing import break_chunks, DataProcessor
from util import SydGrid,LangCodes,total_language_count

# Get start time for the program
STARTTIME = dt.now()
ENDTIME = None

# Start doing MPI
COMM = MPI.COMM_WORLD
SIZE = COMM.Get_size()
RANK = COMM.Get_rank()

sydGrid = "sydGrid.json"

argParser = argparse.ArgumentParser()
argParser.add_argument('--dataset', type = str, default = 'bigTwitter.json')
argParser.add_argument('--langcodes', type = str, default = 'language_codes.json')
args = argParser.parse_args()

here = os.path.dirname(os.path.abspath(__file__))
dataSetPath = os.path.join(here, args.dataset)
codesPath = os.path.join(here, args.langcodes)


lc = LangCodes(codesPath)

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
    chunk_per_process = COMM.scatter(chunks, root=0)
    """
    print("Rank " + str(RANK) + " received chunk - chunkStart: " + str(
        chunk_per_process['chunkStart']) + " -  chunkSize " +
          str(chunk_per_process['chunkSize']))
    """
    # Start processing of chunk
    dataProcessor.process_wrapper(dataSetPath,
                                   chunk_per_process["chunkStart"],
                                   chunk_per_process["chunkSize"])
   
    worker_results = dataProcessor.get_results()
    
    ## Still waiting on helper functions to be finished and defined before we can fill in blanks
    
    if RANK != 0:
        ENDTIME = dt.now()
    worker_results = COMM.gather(worker_results, root=0)

    COMM.Barrier()
    
    if RANK==0:
        counter_lang = dict(Counter())
        for item in worker_results:
            for lang in item.keys():
                if lang not in counter_lang.keys():
                    counter_lang[lang] = item[lang] 
                else:
                    counter_lang[lang] = counter_lang[lang] + item[lang] 
    
    print("")
    print("Final results")
    total_language_count(counter_lang, lc)
    ENDTIME = dt.now()
    print("Total execution time was: " + str(ENDTIME - STARTTIME))
    ## Still waiting on helper functions to be finished and defined before we can fill in blanks

if __name__ == "__main__":
    main()