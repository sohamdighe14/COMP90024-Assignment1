#!/usr/bin/env python
import argparse
import os
from datetime import datetime as dt
from collections import Counter
from mpi4py import MPI
from data_preprocessing import breakChunks, TweetProcessor
from util import LangCodes,total_language_count

# Recording startime for the execution
STARTTIME = dt.now()
ENDTIME = None

# Start parellization
COMM = MPI.COMM_WORLD
SIZE = COMM.Get_size()
RANK = COMM.Get_rank()

sydGrid = "sydGrid.json"

#getting arguments from the slurm scripts
argParser = argparse.ArgumentParser()
argParser.add_argument('--dataset', type = str, default = 'bigTwitter.json')
argParser.add_argument('--langcodes', type = str, default = 'language_codes.json')
args = argParser.parse_args()

# joining os directory to get current directory in the filesystem
here = os.path.dirname(os.path.abspath(__file__))
dataSetPath = os.path.join(here, args.dataset)
codesPath = os.path.join(here, args.langcodes)

# loading language codes
lc = LangCodes(codesPath)

def main():
    # calling the class doing the actual processing
    tweetProcessor = TweetProcessor()
    if RANK == 0:
        dataTotSize = os.path.getsize(dataSetPath)
        #dividing the file in almost equal chunks 
        sizePerProcess = dataTotSize / SIZE
        
        chunks = []
        for chunkStart, chunkSize in breakChunks(dataSetPath,
                                              int(sizePerProcess),
                                              dataTotSize):
            chunks.append({"chunkStart": chunkStart, "chunkSize": chunkSize})
    else:
        chunks = None

    # waiting for all processes to be ready
    COMM.Barrier()
    chunk_per_process = COMM.scatter(chunks, root=0)

    # Start processing of chunk
    tweetProcessor.process_wrapper(dataSetPath,
                                   chunk_per_process["chunkStart"],
                                   chunk_per_process["chunkSize"])
   
    worker_result = tweetProcessor.get_results()
    
    # if the worker is not the root node then we just send results back to the root    
    if RANK != 0:
        ENDTIME = dt.now()
    worker_results = COMM.gather(worker_result, root=0)
    # waiting for all processes to complete sending results back
    COMM.Barrier()
    
    if RANK==0:
        counter_lang = dict(Counter())
        for item in worker_results:
            for lang in item.keys():
                if lang not in counter_lang.keys():
                    counter_lang[lang] = item[lang] 
                else:
                    counter_lang[lang] = counter_lang[lang] + item[lang] 
        # printing final output 
        print("")
        print("Final results")
        total_language_count(counter_lang, lc)
        ENDTIME = dt.now()
        print("Total execution time was: " + str(ENDTIME - STARTTIME))

if __name__ == "__main__":
    main()