#!/usr/bin/env python
import json 
from collections import Counter
from util import GetData

def breakChunks(fpath, chunkSize, totalSize):
    """
    Break the file into into separate batches to be processed by different
    processors
    keyword:
    chunkSize - the size of each chunk
    totalSize - the total size of the file
    """
    with open(fpath,'rb') as f:
        #position pointer for file 
        chunkEnd = f.tell()

        while True:
            # we update this for every   
            chunkStart = chunkEnd
            # define ending of the chunk
            f.seek(f.tell()+chunkSize)
            # read everyline 
            f.readline()
            #checking if the line is inside the chunk 
            chunkEnd = f.tell()
            #getting the size required
            if chunkEnd > totalSize:
                chunkEnd = totalSize
            yield chunkStart, chunkEnd-chunkStart
            if chunkEnd == totalSize:
                break


def breakBatches(fpath, chunkStart, chunkSize, batchSize):
    with open(fpath, 'rb') as f:
        batchEnd = chunkStart

        while True:
            batchStart = batchEnd
            # go to current position + batch_size
            f.seek(batchStart + batchSize)
            # read the lines as a whole
            f.readline()
            batchEnd = f.tell()
            if batchEnd > chunkStart + chunkSize:
                batchEnd = chunkStart + chunkSize
            yield batchStart, batchEnd - batchStart
            # reaching the end of chunk
            if batchEnd == chunkStart + chunkSize:
                break


class TweetProcessor():

    def __init__(self,batch_size = 1024):
        self.batch_size = batch_size #BATCH_SIZE in repo
        self.lang_counter = dict(Counter())

    def get_results(self): #retrive_results in repo
        return self.lang_counter

    def process_tweet(self, tweet):
        """Process tweet and perform counting operations
        Keyword arguments:
        tweet -- tweet in JSON format
        """
        # Extract language
        data = GetData(tweet)
        if data is not None:
            language = data['language']
            cell = data['grid_cell']
            if cell not in self.lang_counter.keys():
                self.lang_counter[cell] = Counter({language:1})
            else :
                self.lang_counter[cell] = self.lang_counter[cell] + Counter({language:1})
            
    def process_wrapper(self, path_to_dataset, chunk_start, chunk_size):
        """Main method executed by worker processes to split file chunk into smaller
        batches and then process the batches sequentially
        Keyword arguments:
        path_to_dataset -- Path to dataset to be split up
        chunk_start -- Byte offset of chunk from beginning of file
        chunk_size -- Size of chunk in bytes
        """
        with open(path_to_dataset, 'rb') as f:
            batches = []

            # Split up chunk into batches of size BATCH_SIZE
            for read_start, read_size in breakBatches(path_to_dataset, chunk_start, chunk_size, self.batch_size):
                batches.append({"batchStart": read_start, "batchSize": read_size})

            # Process batches sequentially
            for batch in batches:

                # Move to start position of batch
                f.seek(batch['batchStart'])

                if batch['batchSize'] > 0:
                    # Read in next batch in bytes as given per batchSize and
                    # split lines
                    content = f.read(batch['batchSize']).splitlines()

                    for line in content:
                        # Decode each line as utf-8 string
                        line = line.decode('utf-8')  # Convert to utf-8
                        if line[-1] == ",":  # if line has comma
                            line = line[:-1]  # removing trailing comma
                        try:
                            # Load tweet in JSON format
                            tweet = json.loads(line)
                            self.process_tweet(tweet)

                        except Exception as e:
                            print(e)
                            print("Error reading row from JSON file - ignoring")
                            print(line)
                else:
                    print("batchsize with size 0 detected")