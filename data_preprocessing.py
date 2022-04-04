#!/usr/bin/env python
import json 
from collections import Counter
from util import GetData

def break_chunks(file_name, chunk_size, total_size):
    with open(file_name,'rb') as f:
        #position pointer for file 
        chunk_end = f.tell()

        while True:
            # we update this for every   
            chunk_start = chunk_end
            # define ending of the chunk
            f.seek(f.tell()+chunk_size)
            # read everyline 
            f.readline()
            #checking if the line is inside the chunk 
            chunk_end = f.tell()
            #getting the size required
            if chunk_end > total_size:
                chunk_end = total_size
            yield chunk_start, chunk_end-chunk_start
            if chunk_end == total_size:
                break


def break_batches(file_path, chunk_start, chunk_size, batch_size):
    with open(file_path, 'rb') as f:
        batch_end = chunk_start

        while True:
            batch_start = batch_end
            # go to current position + batch_size
            f.seek(batch_start + batch_size)
            # read the lines as a whole
            f.readline()
            batch_end = f.tell()
            if batch_end > chunk_start + chunk_size:
                batch_end = chunk_start + chunk_size
            yield batch_start, batch_end - batch_start
            # reaching the end of chunk
            if batch_end == chunk_start + chunk_size:
                break


class DataProcessor():

    def __init__(self, batch_size = 1024):
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
        language = data['language']
        cell = data['grid_cell']
        self.lang_counter[cell][language] += 1
        

    def process_wrapper(self, path_to_dataset, chunk_start, chunk_size):
        """Main method executed by worker process to split chunk into smaller
        batches and process batches sequentially
        Keyword arguments:
        path_to_dataset -- Path to dataset to be split up
        chunk_start -- Byte offset of chunk from beginning of file
        chunk_size -- Size of chunk in bytes
        """
        with open(path_to_dataset, 'rb') as f:
            batches = []

            # Split up chunk into batches of size BATCH_SIZE
            for read_start, read_size in break_batches(path_to_dataset, chunk_start, chunk_size, self.batch_size):
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
                            print("Error reading row from JSON file - ignoring")
                            print(line)
                else:
                    print("batchsize with size 0 detected")