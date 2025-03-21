import openai
from openai import OpenAI

  # import ChatCompletion directly
from src.agent import Agent
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
from typing import List, Callable
import dataclasses
from copy import deepcopy


class OpenAIChatCompletion(Agent):
    def __init__(self, api_args=None, **config):
        if not api_args:
            api_args = {}
        print("api_args={}".format(api_args))
        print("config={}".format(config))

        api_args = deepcopy(api_args)
        api_key = api_args.pop("key", None) or os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OpenAI API key is required, please assign api_args.key or set OPENAI_API_KEY environment variable.")
        self.client = OpenAI(api_key=api_key)
        os.environ['OPENAI_API_KEY'] = api_key
        print("OpenAI API key={}".format(openai.api_key))
        api_base = api_args.pop("base", None) or os.getenv('OPENAI_API_BASE')
        if api_base:
            os.environ['OPENAI_API_BASE'] = api_base
        print("openai api_base={}".format(api_base))
        api_args["model"] = api_args.pop("model", None)
        if not api_args["model"]:
            raise ValueError("OpenAI model is required, please assign api_args.model.")
        self.api_args = api_args
        super().__init__(**config)

    def inference(self, history: List[dict]) -> str:
        # Make a deep copy of history so that modifying it won't affect the original.
        history = json.loads(json.dumps(history))
        for h in history:
            if h['role'] == 'agent':
                h['role'] = 'assistant'

        # Call the chat completion endpoint via the directly imported ChatCompletion class.
        resp = self.client.chat.completions.create(messages=history,
        **self.api_args)

        return resp.choices[0].message.content


class OpenAICompletion(Agent):
    def __init__(self, api_args=None, **config):
        if not api_args:
            api_args = {}
        api_args = deepcopy(api_args)
        api_key = api_args.pop("key", None) or os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OpenAI API key is required, please assign api_args.key or set OPENAI_API_KEY environment variable.")
        self.client = OpenAI(api_key=api_key)
        os.environ['OPENAI_API_KEY'] = api_key
        print("OpenAI API key={}".format(openai.api_key))
        api_base = api_args.pop("base", None) or os.getenv('OPENAI_API_BASE')
        if api_base:
            os.environ['OPENAI_API_BASE'] = api_base
            # TODO: The 'openai.api_base' option isn't read in the client API. You will need to pass it when you instantiate the client, e.g. 'OpenAI(base_url=api_base)'
            # openai.api_base = api_base
        print("openai api_base={}".format(api_base))
        api_args["model"] = api_args.pop("model", None)
        if not api_args["model"]:
            raise ValueError("OpenAI model is required, please assign api_args.model.")
        self.api_args = api_args
        super().__init__(**config)

    def inference(self, history: List[dict]) -> str:
        prompt = ""
        for h in history:
            role = 'Assistant' if h['role'] == 'agent' else h['role']
            content = h['content']
            prompt += f"{role}: {content}\n\n"
        prompt += 'Assistant: '

        resp = self.client.completions.create(prompt=prompt,
        **self.api_args)

        return resp.choices[0].text

