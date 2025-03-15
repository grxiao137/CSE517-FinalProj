#!/bin/bash

default_agent="gpt-3.5-turbo"
wks="1"

if [[ "$1" == "gpt-3.5-turbo" || "$1" == "gpt-4o" ]]; then
    agent_config="configs/agents/api_agents/$1.yaml"
else
    echo "Invalid or missing agent type. Using default: gpt-3.5-turbo.yaml"
    agent_config="configs/agents/api_agents/${default_agent}.yaml"
fi

python eval.py --task configs/tasks/knowledgegraph/ext.yaml --agent configs/agents/api_agents/gpt-3.5-turbo.yaml --workers "$wks"

