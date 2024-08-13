from tqdm import tqdm
import multiprocessing
import json
import os
from utils import *
import datatrove
from datatrove.pipeline.filters import (
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
)
from datatrove.data import Document
import copy

def apply_filter(filter_for_fineweb, dataset:datatrove.data.Document):
    after = []
    filtered = []
    whys = []
    for i in tqdm(dataset):
        if filter_for_fineweb.filter(i)==True:
            after.append(i)
        else:
            why = str(filter_for_fineweb.filter(i))
            whys.append(why)
            filtered.append(i)
    print(f'size of dataset before filtering - {len(dataset)}')
    print(f'size of dataset after filtering - {len(after)}')
    return after, filtered, whys


def load_jsonl(path):
    result = []
    f = open(path,'r',encoding = 'utf-8')
    for i in f:
        try:
            result.append(json.loads(i))
        except:
            continue
    return result 

def three_way_filter(data_i):
    huh = filtering_huh(data_i)
    ok = ok_filter.filter(data_i['text'])
    hy = filteringHY(data_i)
    return huh, ok, hy

ok_filter = OKGopherRepetitionFilter()
def map_filter(in_path, out_path_1, out_path_2):
    ff1 = open(out_path_1, 'w', encoding='utf-8')
    ff2 = open(out_path_2, 'w', encoding='utf-8')
    with open(in_path, 'r', encoding='utf-8') as f:
        for i in f:
            tmp = json.loads(i)
#             print(tmp)
            huh, ok, hy = three_way_filter(tmp)
            tmp['huh']=huh
            tmp['ok']=ok
            tmp['hy']=hy
#             print(tmp)
            if huh==True and ok==True and hy==True:
                ff1.write(json.dumps(tmp,ensure_ascii=False)+'\n') # for korean
            else:
                ff2.write(json.dumps(tmp, ensure_ascii=False)+'\n')
    ff1.close()
    ff2.close()

import argparse

def get_args():
    # parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--unfiltered_output_dir', type=str, help = 'output 위치')
    parser.add_argument('--input_dir', type=str, help = 'output 위치')
    parser.add_argument('--filtered_output_dir', type=str, help = 'output 위치')
    
    args  = parser.parse_args()
    return args

if __name__=='__main__':
    args = get_args()
    path = args.input_dir
    data_list = [i for i in os.listdir(path) if i.endswith('part')]
    cpu_count = os.cpu_count()
    os.makedirs(args.unfiltered_output_dir, exist_ok=True)
    os.makedirs(args.filtered_output_dir, exist_ok=True)
    
    now = time.time()
    pool = multiprocessing.Pool(processes=cpu_count)
    print(len(data_list))
    for i in tqdm(range(0,len(data_list),cpu_count)):
        in_paths = [os.path.join(path,j) for j in data_list[i:i+cpu_count]]
        out_paths_1 = [os.path.join(args.unfiltered_output_dir, os.path.splitext(j)[0]) for j in data_list[i:i+cpu_count]]
        out_paths_2 = [os.path.join(args.filtered_output_dir, os.path.splitext(j)[0]) for j in data_list[i:i+cpu_count]]
        inputs = [(a,b,c) for a,b,c in zip(in_paths, out_paths_1, out_paths_2)]
        pool.starmap(map_filter, inputs)#['p1','p2','p3','p4'])
    print(time.time()-now)
    pool.close()
    pool.join()
