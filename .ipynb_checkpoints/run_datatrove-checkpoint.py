from utils import *
import os
from typing import Callable, Literal
from datatrove.io import DataFileLike, DataFolderLike
from datatrove.pipeline.readers.base import BaseDiskReader
from datatrove.utils.logging import logger
from datatrove.pipeline.filters import (
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
)
from datatrove.pipeline.readers import CSVReader,JsonlReader
from datatrove.pipeline.filters import SamplerFilter
from datatrove.pipeline.writers import JsonlWriter
from datatrove.executor import LocalPipelineExecutor
import argparse
import time

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str)#, default="/home/work/user/ocw/truthfulqa/data/ibk/ibk_v5.2.2.jsonl")
    parser.add_argument("--output_dir", type=str)#, default="./output")
    args = parser.parse_args()
    return args

    
if __name__ == '__main__':
    args = get_args()
    path = args.data_path #'/home/work/user/wjpark/midm_pack/data/2014-23/remove_duplicate'
    name = os.path.split(os.path.split(path)[0])[1]
    print(path)
    output_path = os.path.join(args.output_dir, name)
    os.makedirs(output_path, exist_ok=True)
    
    os.makedirs(os.path.join(output_path,'removed/1_url'), exist_ok=True)
    os.makedirs(os.path.join(output_path,'removed/3_gopher_rep'), exist_ok=True)
    os.makedirs(os.path.join(output_path,'removed/4_gopher_qual'), exist_ok=True)
    os.makedirs(os.path.join(output_path,'removed/5_c4'), exist_ok=True)
    now = time.time()
    pipeline = [
        JsonlReader(
            data_folder=path
        ),
        URLFilter(exclusion_writer=JsonlWriter(os.path.join(output_path,'removed/1_url'),compression=None)),
        GopherRepetitionFilter(language='ko',exclusion_writer=JsonlWriter(os.path.join(output_path,'removed/3_gopher_rep'),compression=None)),
        GopherQualityFilter(min_stop_words=None,language='ko',exclusion_writer=JsonlWriter(os.path.join(output_path,'removed/4_gopher_qual'),compression=None)),
        C4QualityFilter(filter_no_terminal_punct=False,language='ko',exclusion_writer=JsonlWriter(os.path.join(output_path,'removed/5_c4'),compression=None)),
        JsonlWriter(
            output_folder=os.path.join(args.output_dir,name),
            compression=None
        )
    ]
    num_tasks = len([j for j in os.listdir(path) if j.endswith('part')])
    executor = LocalPipelineExecutor(
        pipeline=pipeline,
        logging_dir=os.path.join(args.output_dir,f"logs/{name}"),
        tasks=num_tasks,
        workers=os.cpu_count()-2
    )
    executor.run()
    print(time.time()-now)