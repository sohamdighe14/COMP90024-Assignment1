#!/usr/bin/env python
import json 
import os

def SydGrid(file_name):
    '''
    "geo":{"type":"Point","coordinates":[-33.86751,151.20797]}
    all 8 acesss and read same time
    datastructures to optimize performance
    showq -q snowy| less
    master has rank 0 always
    sbatch <script_name> 
    squeue -u <username>
    module avail mpip4py//list all libs
    tell module load <lib> in slurm script
    scancel <job_id>
    '''
    syd_grid = []
    with open(file_name) as f:
        data = json.load(f)
        for val in data['features']:
            grid_data = {}
            properties = val['properties']
            grid_data['id'] = properties['id']
            num = int((grid_data['id'])%4)
            if num == 0:
                num=4
            grid_data['cell'] = chr(int(num+64))+str(int((grid_data['id']-1)/4)+1)
            geometry = val['geometry'] 
            grid_data['xmin'] = geometry['coordinates'][0][0][0]
            grid_data['ymin'] = geometry['coordinates'][0][2][1]
            grid_data['xmax'] = geometry['coordinates'][0][1][0]
            grid_data['ymax'] = geometry['coordinates'][0][0][1]
            syd_grid.append(grid_data)
    return syd_grid


def LangCodes(file_name):
    lang_codes = dict()
    with open(file_name, encoding='utf-8') as f:
            data = json.load(f) 
            for val in data:
                lang_codes[val['code']] = val['name']
            return lang_codes


def GetGridCell(x,y):
    here = os.path.dirname(os.path.abspath(__file__))
    gridPath = os.path.join(here, './sydGrid.json')
    grid = SydGrid(gridPath)
    for item in grid:
        if item['xmin']<x<=item['xmax'] and item['ymin']<y<=item['ymax']:
            return item['cell']


def GetData(tweet):
    XMAX = 151.9925508053294
    XMIN = 149.79255080532937
    YMAX = -32.81644989181766
    YMIN = -34.81644989181766
    tweet_data = None
    if tweet['doc']['geo'] is not None:
        language = tweet['doc']['metadata']['iso_language_code']
        x = tweet['doc']['geo']['coordinates'][1]
        y = tweet['doc']['geo']['coordinates'][0]
        #checking if the point lies in grid
        if XMAX > x > XMIN and YMAX > y > YMIN:
            tweet_data = {}
            tweet_data['language'] = language
            tweet_data['grid_cell'] = GetGridCell(x,y)
    return tweet_data


def total_language_count(dict_lang, lang_codes):
    """Print the results from language code counting.
    Keyword arguments:
    counter_language -- counter with language counts
    supported_languages -- dict mapping lancode to name
    """
    for k,v in dict_lang.items():
        count = 1
        print("Cell :",k)
        print('# tweets', sum(v.values()))
        print('# languages :', len(v))
        counter_language = v
        counter = 0
        for key, val in v.items():
            if key in lang_codes.keys() and counter<10:
                print(str(count) + ". " + lang_codes[key] + " (" + key+ ")" + ", " + str(val))
                count += 1
                counter +=1
