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

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str)#, default="/home/work/user/ocw/truthfulqa/data/ibk/ibk_v5.2.2.jsonl")
    parser.add_argument("--output_dir", type=str)#, default="./output")
    args = parser.parse_args()
    return args


# class JsonlReader(BaseDiskReader):
#     """Read data from JSONL files.
#         Will read each line as a separate document.

#     Args:
#         data_folder: a str, tuple or DataFolder object representing a path/filesystem
#         paths_file: optionally provide a file with one path per line (without the `data_folder` prefix) to read.
#         compression: the compression to use (default: "infer")
#         limit: limit the number of documents to read. Useful for debugging
#         skip: skip the first n rows
#         file_progress: show progress bar for files
#         doc_progress: show progress bar for documents
#         adapter: function to adapt the data dict from the source to a Document.
#             Takes as input: (self, data: dict, path: str, id_in_file: int | str)
#                 self allows access to self.text_key and self.id_key
#             Returns: a dict with at least a "text" and "id" keys
#         text_key: the key containing the text data (default: "text").
#         id_key: the key containing the id for each sample (default: "id").
#         default_metadata: a dictionary with any data that should be added to all samples' metadata
#         recursive: whether to search files recursively. Ignored if paths_file is provided
#         glob_pattern: pattern that all files must match exactly to be included (relative to data_folder). Ignored if paths_file is provided
#         shuffle_files: shuffle the files within the returned shard. Mostly used for data viz. purposes, do not use with dedup blocks
#     """

#     name = "🐿 Jsonl"
#     _requires_dependencies = ["orjson"]

#     def __init__(
#         self,
#         data_folder: DataFolderLike,
#         paths_file: DataFileLike | None = None,
#         compression: Literal["infer", "gzip", "zstd"] | None = "infer",
#         limit: int = -1,
#         skip: int = 0,
#         file_progress: bool = False,
#         doc_progress: bool = False,
#         adapter: Callable = None,
#         text_key: str = "text",
#         id_key: str = "id",
#         default_metadata: dict = None,
#         recursive: bool = True,
#         glob_pattern: str | None = None,
#         shuffle_files: bool = False,
#     ):
#         super().__init__(
#             data_folder,
#             paths_file,
#             limit,
#             skip,
#             file_progress,
#             doc_progress,
#             adapter,
#             text_key,
#             id_key,
#             default_metadata,
#             recursive,
#             glob_pattern,
#             shuffle_files,
#         )
#         self.compression = compression

#     def read_file(self, filepath: str):
#         import orjson
#         from orjson import JSONDecodeError

#         with self.data_folder.open(filepath, "r", compression=self.compression) as f:
#             try:
#                 for li, line in enumerate(f):
#                     with self.track_time():
#                         try:
#                             document = self.get_document_from_dict(orjson.loads(line), filepath, li)
#                             document.text=document.text.replace('<[!newline]>\n','\n')
#                             if not document:
#                                 continue
#                         except (EOFError, JSONDecodeError) as e:
#                             logger.warning(f"Error when reading `{filepath}`: {e}")
#                             continue
#                     yield document
#             except UnicodeDecodeError as e:
#                 logger.warning(f"File `{filepath}` may be corrupted: raised UnicodeDecodeError ({e})")

    
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
        logging_dir=f"logs/{name}",
        tasks=num_tasks,
        workers=os.cpu_count()-2
    )
    executor.run()