import jsonlines
import traceback
import threading
import numpy as np
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from typing import Dict, Callable, Type, Tuple, List, Any, Union, Iterable, Generic, TypeVar
from abc import ABC, abstractmethod
from glob import glob
from os.path import join, relpath
from collections import defaultdict
import os
import json
import sys
import time
import re
import math
import random
import datetime
import argparse
import requests

# from .utils import print_rank_0
# from .model_api import get_model_api
from .agent import Agent, Session
from .utils import serialize

import signal

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException("Iteration took too long")


signal.signal(signal.SIGALRM, timeout_handler)
signal.siginterrupt(signal.SIGALRM, True)


T_INPUT = TypeVar('T_INPUT')
T_OUTPUT = TypeVar('T_OUTPUT')
T_TARGET = TypeVar('T_TARGET')


class DataPiece(Generic[T_INPUT, T_TARGET]):
    def __init__(self, input: T_INPUT, target: T_TARGET):
        self.input = input
        self.target = target


class Dataset(Generic[T_INPUT, T_TARGET], List[DataPiece[T_INPUT, T_TARGET]]):
    def get_inputs(self) -> List[T_INPUT]:
        return [item.input for item in self]

    def get_targets(self) -> List[T_TARGET]:
        return [item.target for item in self]


class Task(Generic[T_INPUT, T_OUTPUT, T_TARGET]):
    def __init__(self, **kwargs):
        self.name = kwargs.pop("name", None)
        self.worker_limit = kwargs.pop("worker_limit", None)
        self.workers = kwargs.pop("workers", 1)
        self.src = kwargs.pop("src", None)
        self.output_root_dir = kwargs.pop("output_dir", None)
        assert isinstance(self.workers, int) and self.workers > 0
        assert isinstance(self.name, str)

    def release(self):
        pass

    def evaluate(self, agent: Agent, already_runs: List[Any]=None) -> Dict[str, Any]:
        print(f"Evaluating task '{self.name}' ...")
        data = self.get_data()
        inputs = data.get_inputs()
        targets = data.get_targets()
        try:
            results = self.predict_all(agent, inputs, already_runs)
        except:
            results = self.predict_all(agent, inputs)
        result_dict = {}
        for metric in self.metrics:
            result_dict[metric] = self.metrics[metric](results, targets)
        print(f"Task '{self.name}' evaluation finished. The results are saved in '{self.get_output_dir()}'")
        self.save_runs_all(inputs, results, targets, result_dict)
        return result_dict

    '''
    def predict_all(self, agent: Agent, inputs: List[T_INPUT], already_runs: List[Any]=None) -> List[T_OUTPUT]:
        print(f"Start Predicting All {len(inputs)} ...")
        assert already_runs is None or len(already_runs) == len(inputs)

        thread_count = self.workers
        if self.worker_limit:
            thread_count = min(self.workers, self.worker_limit)

        executor = ThreadPoolExecutor(max_workers=thread_count)

        threads = []
        results = [None] * len(inputs)

        def call_wrap(data_item, index):
            signal.alarm(5)
            try:
                session = agent.create_session()
                # Attempt to run predict_single. If it takes > 120s, a TimeoutException will be raised.
                result = self.predict_single(session, data_item)
                self.save_single(index, data_item, result, session)
            except TimeoutException:
                print(f"Timeout: iteration {index} exceeded 100 sec. Skipping this iteration.")
                result = None  # or any default value you prefer when skipping
            except Exception as e:
                # Optionally print traceback or log the exception.
                import traceback
                traceback.print_exc()
                result = None
            finally:
                # Always cancel the alarm to avoid it triggering later.
                signal.alarm(0)
            results[index] = result

        for idx, item in enumerate(inputs):
            if already_runs is not None and already_runs[idx] is not None:
                results[idx] = already_runs[idx]
                continue
            future = executor.submit(call_wrap, item, idx)
            threads.append(future)
        
        with tqdm(total=len(inputs)) as pbar:
            for thread in as_completed(threads):
                pbar.update(1)

        return results
    '''
    def predict_all(self, agent: Agent, inputs: List[T_INPUT], already_runs: List[Any] = None) -> List[T_OUTPUT]:
        print(f"Start Predicting All {len(inputs)} ...")
        # Ensure the already_runs list has the same length as inputs, if provided.
        assert already_runs is None or len(already_runs) == len(inputs)

        results = [None] * len(inputs)

        def call_wrap(data_item, index):
            # Set an alarm for 5 seconds (adjust the timeout value as needed).
            signal.alarm(100)
            try:
                session = agent.create_session()
                # Attempt to run predict_single. If it takes longer than 5 seconds,
                # a TimeoutException will be raised.
                result = self.predict_single(session, data_item)
                self.save_single(index, data_item, result, session)
            except TimeoutException:
                print(f"Timeout: iteration {index} exceeded 100 seconds. Skipping this iteration.")
                result = None  # Use None or another default value for timeouts.
            except Exception as e:
                # Optionally print the traceback or log the exception.
                import traceback
                traceback.print_exc()
                result = None
            finally:
                # Always cancel the alarm to avoid it triggering later.
                signal.alarm(0)
            results[index] = result

        # Process each input sequentially.
        for idx, item in tqdm(enumerate(inputs), total=len(inputs)):
            if already_runs is not None and already_runs[idx] is not None:
                results[idx] = already_runs[idx]
            else:
                call_wrap(item, idx)

        return results


    def save_single(self, index: int, input: T_INPUT, output: T_OUTPUT, session: Session=None):
        save_obj = {
            "index": index,
            "input": serialize(input),
            "output": serialize(output),
            "history": session.history if session else None,
            "exception_raised": session.exception_raised if session else None
        }
        if not os.path.exists(self.get_output_dir()):
            try: os.makedirs(self.get_output_dir())
            except: pass
        with open(os.path.join(self.get_output_dir(), "generation.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(save_obj, ensure_ascii=False) + "\n")

    def save_runs_all(self, inputs: List[T_INPUT], outputs: List[T_OUTPUT], targets: List[T_TARGET], metrics: Dict[str, Any] = None):
        if not os.path.exists(self.get_output_dir()):
            try: os.makedirs(self.get_output_dir())
            except: pass
        for idx, (input, output, target) in enumerate(zip(inputs, outputs, targets)):
            save_obj = {
                "index": idx,
                "input": serialize(input),
                "output": serialize(output),
                "target": serialize(target)
            }
            with open(os.path.join(self.get_output_dir(), "runs.jsonl"), "a", encoding="utf-8") as f:
                f.write(json.dumps(save_obj, ensure_ascii=False) + "\n")
        self.save_metrics_all(metrics)

    def save_metrics_all(self, metrics: Dict[str, Any]):
        if not os.path.exists(self.get_output_dir()):
            try: os.makedirs(self.get_output_dir())
            except: pass
        with open(os.path.join(self.get_output_dir(), "results.json"), "w", encoding="utf-8") as f:
            f.write(json.dumps(metrics, ensure_ascii=False, indent=4))

    def get_output_dir(self) -> str:
        """
            Default output directory is: outputs/{time_str}/{name or category}
        """
        if not self.output_root_dir:
            self.output_root_dir = "outputs/%s/%s" % (datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"), self.name or "default")
        return self.output_root_dir

    @property
    def metrics(self) -> Dict[str, Callable[[List[T_OUTPUT], List[T_TARGET]], Any]]:
        return {"EM": lambda outputs, targets: len([1 for o, t in zip(outputs, targets) if o == t]) / min(len(outputs), len(targets))}

    def get_data(self) -> Dataset[T_INPUT, T_TARGET]:
        raise NotImplementedError

    def predict_single(self, session: Session, data_item: T_INPUT) -> T_OUTPUT:
        raise NotImplementedError
