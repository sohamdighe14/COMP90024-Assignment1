import json 
import pandas as pd
def SydGrid(file_name='./sydGrid.json'):
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
grid = SydGrid()
syd_grid = pd.DataFrame(grid)