from tqdm import tqdm
import multiprocessing
import json
import os
import copy
import datatrove
from utils import *
import argparse
from datatrove.pipeline.filters import (
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
)
from datatrove.data import Document
from typing import List
import time

def get_args():
    # parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_dir', type=str, help = 'output 위치',default='/home/work/user/ocw/kt_output/heuristic_filtering/2021-04/test')
    parser.add_argument('--input_dir', type=str, help = 'output 위치',default='/home/work/user/wjpark/midm_pack/data/2021-04/remove_duplicate')
    parser.add_argument('--filter_name', type=str, help = 'output 위치',default='url')
    args  = parser.parse_args()
    return args

def apply_filter(filter_for_fineweb, dataset:List[datatrove.data.Document], filter_name):
    after = []
    filtered = []
    for i in tqdm(dataset,desc=filter_name):
        if filter_for_fineweb.filter(i)==True:
            after.append(i)
        else:
            filtered.append(i)
    print(f'size of dataset before {filter_name} filtering - {len(dataset)}')
    print(f'size of dataset after {filter_name} filtering - {len(after)}')
    return after, filtered

def map_filter(in_path, out_path):
    name = (os.path.split(in_path[0])[1]).split('jsonl')[0]
    output_path = os.path.split(out_path)[0]
    # read
    dataset = load_jsonl(in_path)
    dataset = [Document(text=i['text'], id=i['adlr_id'], metadata=i) for i in dataset]
    # url
    url_filter=URLFilter()
    dataset_url, filtered = apply_filter(url_filter, dataset, 'url')
    # save
    filtered = [i.metadata for i in filtered]
    save_jsonl(os.path.join(output_path, 'url/removed'), filtered, name)
    
    gopher_repetition_filter = GopherRepetitionFilter(language='ko')
    dataset_gopher_repetition, filtered = apply_filter(gopher_repetition_filter, dataset_url, 'gopher_rep')
    # save
    filtered = [i.metadata for i in filtered]
    save_jsonl(os.path.join(output_path, 'url/gopher_rep'), filtered, name)
    
    gopher_quality_filter = GopherQualityFilter(min_stop_words=None,language='ko')
    dataset_gopher_quality, filtered = apply_filter(gopher_quality_filter, dataset_gopher_repetition, 'gopher_quality')
    # save
    filtered = [i.metadata for i in filtered]
    save_jsonl(os.path.join(output_path, 'url/gopher_quality'), filtered, name)
    
    c4_filter = C4QualityFilter(filter_no_terminal_punct=False,language='ko')
    dataset_c4, filtered = apply_filter(c4_filter, dataset_gopher_quality, 'c4')
    # save
    filtered = [i.metadata for i in filtered]
    save_jsonl(os.path.join(output_path, 'url/c4'), filtered, name)
    
    # output
    output = [i.metadata for i in c4_filter]
    save_jsonl(output_path, output, name)

if __name__=='__main__':
    args = get_args()
    path = args.input_dir
    output_dir = args.output_dir
    filter_name = args.filter_name
    data_list = [i for i in os.listdir(path) if i.endswith('part')]
    cpu_count = os.cpu_count()
    os.makedirs(output_dir, exist_ok=True)
    for filter_name in ['url','gopher_rep','gopher_quality','c4']:
        os.makedirs(os.path.join(output_dir,f'{filter_name}/removed'), exist_ok=True)    
    
    now = time.time()
    pool = multiprocessing.Pool(processes=cpu_count)

    for i in tqdm(range(0,len(data_list),cpu_count)):
        in_paths = [os.path.join(path, j) for j in data_list[i:i+cpu_count]]
        out_paths = [os.path.join(output_dir, os.path.splitext(j)[0]) for j in data_list[i:i+cpu_count]]
        inputs = [(a,b) for a,b in zip(in_paths, out_paths)]
        pool.starmap(map_filter, inputs)#['p1','p2','p3','p4'])
    print(time.time()-now)
    pool.close()
    pool.join()
