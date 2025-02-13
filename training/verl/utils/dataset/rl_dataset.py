# Copyright 2024 Bytedance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from omegaconf import ListConfig
import os
from typing import List, Union

import pandas as pd

import torch
import numpy as np
from torch.utils.data import Dataset, DataLoader
from transformers import AutoTokenizer, PreTrainedTokenizer

from verl import DataProto
from verl.utils.fs import copy_local_path_from_hdfs

from verl.utils.model import compute_position_id_with_mask
import verl.utils.torch_functional as verl_F

import ray
from joblib import Parallel, delayed

# Multi-processing setup for batch tokenization
import multiprocessing
from transformers import AutoTokenizer
from functools import partial
def init_process(tokenizer_name):
    global process_tokenizer
    process_tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)

def process_chunk(max_prompt_length,chunk):
    try:
        length = []
        for msg in  list(chunk):
            tokenized = process_tokenizer.apply_chat_template(
                list(map(dict,msg)),
                add_generation_prompt=True,
                return_tensors=None,
                padding=False,
                truncation=False
            )
            length.append(len(tokenized))
        return length
    except Exception as e:
        print(f"Error processing chunk: {e}")
        return [max_prompt_length + 1] * len(chunk)  # Exclude failed chunks


def collate_fn(data_list: list[dict]) -> dict:
    tensors = {}
    non_tensors = {}

    for data in data_list:
        for key, val in data.items():
            if isinstance(val, torch.Tensor):
                if key not in tensors:
                    tensors[key] = []
                tensors[key].append(val)
            else:
                if key not in non_tensors:
                    non_tensors[key] = []
                non_tensors[key].append(val)

    for key, val in tensors.items():
        tensors[key] = torch.stack(val, dim=0)

    for key, val in non_tensors.items():
        non_tensors[key] = np.array(val, dtype=object)

    output = {}
    output.update(tensors)
    output.update(non_tensors)
    return output


class RLHFDataset(Dataset):
    """
    We assume the dataset contains a column that contains prompts and other information
    """

    def __init__(self,
                 parquet_files: Union[str, List[str]],
                 tokenizer: PreTrainedTokenizer,
                 prompt_key='prompt',
                 max_prompt_length=1024,
                 filter_prompts=True,
                 cache_dir='~/.cache/verl/rlhf',
                 chat_template_func=None,
                 return_raw_chat=False,
                 system_prompt=None,
                 truncation='error'):
        if not isinstance(parquet_files, (List, ListConfig)):
            parquet_files = [parquet_files]

        self.parquet_files = parquet_files
        self.cache_dir = os.path.expanduser(cache_dir)
        self.tokenizer = tokenizer

        self.prompt_key = prompt_key
        self.max_prompt_length = max_prompt_length
        self.filter_prompts = filter_prompts

        self.return_raw_chat = return_raw_chat
        self.chat_template_func = chat_template_func
        self.truncation = truncation
        self.system_prompt = system_prompt

        self._download()
        self._read_files_and_tokenize()

    def _download(self):
        from verl.utils.fs import copy_local_path_from_hdfs
        for i, parquet_file in enumerate(self.parquet_files):
            self.parquet_files[i] = copy_local_path_from_hdfs(src=parquet_file, cache_dir=self.cache_dir)

    # def _read_files_and_tokenize(self):
    #     dataframes = []
    #     for parquet_file in self.parquet_files:
    #         # read parquet files and cache
    #         dataframe = pd.read_parquet(parquet_file)
    #         dataframes.append(dataframe)
    #     self.dataframe = pd.concat(dataframes)

    #     print(f'original dataset len: {len(self.dataframe)}')

    #     # filter out too long prompts
    #     tokenizer = self.tokenizer
    #     prompt_key = self.prompt_key
    #     if self.system_prompt is not None:
    #         self.dataframe[prompt_key] = self.dataframe[prompt_key].apply(lambda doc: doc[0].update({'content': self.system_prompt}) or doc)
    #     self.dataframe = self.dataframe[self.dataframe.apply(lambda doc: len(
    #         tokenizer.apply_chat_template(doc[prompt_key], add_generation_prompt=True)) <= self.max_prompt_length,
    #                                                          axis=1)]

    #     print(f'filter dataset len: {len(self.dataframe)}')

    def _read_files_and_tokenize(self):
        dataframes = []
        for parquet_file in self.parquet_files:
            dataframe = pd.read_parquet(parquet_file)
            dataframes.append(dataframe)
        self.dataframe = pd.concat(dataframes)
        print(f'Original dataset len: {len(self.dataframe)}')

        prompt_key = self.prompt_key

        # Apply system prompt if provided
        if self.system_prompt is not None:
            self.dataframe[prompt_key] = self.dataframe[prompt_key].apply(
                lambda doc: doc[0].update({'content': self.system_prompt}) or doc
            )

        # Extract all prompts for batch processing
        prompts = self.dataframe[prompt_key].tolist()

        tokenizer_name = self.tokenizer.name_or_path
        n_processes = multiprocessing.cpu_count()
        chunk_size = (len(prompts) + n_processes - 1) // n_processes
        chunks = [prompts[i:i + chunk_size] for i in range(0, len(prompts), chunk_size)]

        with multiprocessing.Pool(
            processes=n_processes,
            initializer=init_process,
            initargs=(tokenizer_name,)
        ) as pool:
            chunk_lengths = pool.map(partial(process_chunk, self.max_prompt_length), chunks)

        # Flatten lengths and apply filter
        lengths = [length for sublist in chunk_lengths for length in sublist]
        self.dataframe = self.dataframe[pd.Series(lengths) <= self.max_prompt_length]

        print(f'Filtered dataset len: {len(self.dataframe)}')


    def __len__(self):
        return len(self.dataframe)

    def __getitem__(self, item):
        """
        Note that we also return the raw_input_ids so that it can be combined with other chat template
        """
        row_dict = self.dataframe.iloc[item].to_dict()

        chat = row_dict.pop(self.prompt_key)

        prompt_with_chat_template = self.tokenizer.apply_chat_template(chat, add_generation_prompt=True, tokenize=False)

        input_ids, attention_mask = verl_F.tokenize_and_postprocess_data(prompt=prompt_with_chat_template,
                                                                         tokenizer=self.tokenizer,
                                                                         max_length=self.max_prompt_length,
                                                                         pad_token_id=self.tokenizer.pad_token_id,
                                                                         left_pad=True,
                                                                         truncation=self.truncation)

        position_ids = compute_position_id_with_mask(attention_mask)

        row_dict['input_ids'] = input_ids[0]
        row_dict['attention_mask'] = attention_mask[0]
        row_dict['position_ids'] = position_ids[0]

        # encode prompts without chat template
        if self.return_raw_chat:
            row_dict['raw_prompt'] = chat.tolist()

        return row_dict


class BufferedDataLoader:
    def __init__(self, dataloader):
        self.dataloader = dataloader
        self.batch_size = dataloader.batch_size
        self.buffer = []
        self.dataloader_iter = None

    def start_new_epoch(self):
        """Reset for new epoch"""
        self.dataloader_iter = iter(self.dataloader)

    def get_next_batch(self):
        try:
            return next(self.dataloader_iter)
        except StopIteration:
            raise StopIteration

    def __len__(self):
        return len(self.dataloader)

    def add_to_buffer(self, samples):
        if len(self.buffer) == 0:
            self.buffer = samples
        else:
            self.buffer = DataProto.concat([self.buffer, samples])

    def get_from_buffer(self, count, dp_size):
        if count > self.buffer_size():
            count = (self.buffer_size() // dp_size) * dp_size
        samples = self.buffer.slice(range(0, count))
        self.buffer = self.buffer.slice(range(count, self.buffer_size()))
        return samples

    def buffer_size(self):
        return len(self.buffer)