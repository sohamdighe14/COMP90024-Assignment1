#!/usr/bin/env python
import json 

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
    grid = SydGrid('./sydGrid.json')
    for item in grid:
        if item['xmin']<x<=item['xmax'] and item['ymin']<y<=item['ymax']:
            return item['cell']


def GetData(tweet):
    tweet_data = {}
    if type(tweet['geo'])!= type(None):
        language = tweet['doc']['metadata']['iso_language_code']
        x = tweet['doc']['geo']['coordinates'][1]
        y = tweet['doc']['geo']['coordinates'][0]
        #checking if the point lies in grid
        if 151.992551 < x < 149.792551 and -32.81645 < y < -34.81645:
            tweet_data['language'] = language
            tweet_data['grid_cell'] = GetGridCell(x,y)
    return tweet_data


def total_language_count(dict_lang, lang_codes):
    """Print the results from language code counting.
    Keyword arguments:
    counter_language -- counter with language counts
    supported_languages -- dict mapping lancode to name
    """
    for item in dict_lang:
        print("Cell :",item)
        print('# tweets', sum(dict_lang[item].values()))
        print('# languages :', len(dict_lang[item]))
        counter_language = dict_lang[item]
        count = 1
        print("Most common languages in dataset:", flush=True)
        for language in counter_language.most_common(10):
            #if there is a bug its here
            print(str(count) + ". " + lang_codes[language[0]] + " (" + language[0] + ")" + ", " + str(language[1]), flush=True)
            count += 1