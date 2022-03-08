from datetime import datetime
import re
from time import sleep
import yaml
import pandas as pd
import logging
import operator
from concurrent.futures import ThreadPoolExecutor


def parse_yaml(file_name):
    """Function to parse yaml file"""
    with open(file_name) as f:
        data = yaml.safe_load(f)
    return data


def parse_csv(file_name):
    """Function to parse csv file"""
    df = pd.read_csv(file_name)
    return df


def TimeFunction(name, inp):
    if inp['FunctionInput'][0] == '$':
        var = re.findall(r'\(.*?\)', inp['FunctionInput'])[0][1:-1]
        fun_inp = globals()[var]
    else:
        fun_inp = inp['FunctionInput']
    logging.warning(
        f"{datetime.now()};{name} Executing TimeFunction ({fun_inp}, {inp['ExecutionTime']})")
    sleep(int(inp['ExecutionTime']))


def DataLoad(name, inp):
    logging.warning(
        f"{datetime.now()};{name} Executing DataLoad ({inp['Filename']})")
    data = parse_csv(inp['Filename'])
    return "load", data, data.shape[0]


def Binning(name, inp):
    logging.warning(
        f"{datetime.now()};{name} Executing Binning ({inp['RuleFilename'], inp['DataSet'][2:-1]})")
    bin_data = parse_csv(inp['RuleFilename'])


def execute_flow_serial(name, data):
    logging.warning(f"{datetime.now()};{name} Entry")
    for key, value in data.items():
        if 'Type' in value:
            if value['Type'] == "Flow":
                if value['Execution'] == "Sequential":
                    execute_flow_serial(f"{name}.{key}", value['Activities'])
                elif value['Execution'] == "Concurrent":
                    execute_flow_parallel(f"{name}.{key}", value['Activities'])
            if value['Type'] == "Task":
                execute_task(f"{name}.{key}", value)
    logging.warning(f"{datetime.now()};{name} Exit")


def execute_flow_parallel(name, data):
    logging.warning(f"{datetime.now()};{name} Entry")
    executors = []
    with ThreadPoolExecutor(max_workers=1000) as executor:
        for key, value in data.items():
            if 'Type' in value:
                if value['Type'] == "Flow":
                    if value['Execution'] == "Sequential":
                        executors.append(executor.submit(
                            execute_flow_serial, f"{name}.{key}", value['Activities']))
                    elif value['Execution'] == "Concurrent":
                        executors.append(executor.submit(
                            execute_flow_parallel, f"{name}.{key}", value['Activities']))
                if value['Type'] == "Task":
                    executors.append(executor.submit(
                        execute_task, f"{name}.{key}", value))
    for future in executors:
        future.result()
    logging.warning(f"{datetime.now()};{name} Exit")


def execute_task(name, data):
    ops = {'<': operator.lt, '>': operator.gt}
    logging.warning(f"{datetime.now()};{name} Entry")
    if 'Condition' in data:
        var = re.findall(r'\(.*?\)', data['Condition'])[0][1:-1]
        sym = data['Condition'].split(var)[1].split(' ')[1]
        val = int(data['Condition'].split(var)[1].split(' ')[2])
        while True:
            try:
                if ops[sym](globals()[var], val):
                    return_data = globals()[data['Function']](
                        name, data['Inputs'])
                else:
                    return_data = None
                    logging.warning(f"{datetime.now()};{name} Skipped")
                break
            except:
                pass
    else:
        return_data = globals()[data['Function']](name, data['Inputs'])
    if return_data:
        if return_data[0] == "load":
            globals()[f'{name}.DataTable'] = return_data[1]
            globals()[f'{name}.NoOfDefects'] = return_data[2]
        elif return_data[0] == "bin":
            pass
    logging.warning(f"{datetime.now()};{name} Exit")


if __name__ == "__main__":
    logging.getLogger().handlers.clear()
    logging.basicConfig(filename="M3A.log", format='%(message)s', filemode='w')
    yaml_data = parse_yaml('Milestone3A.yaml')
    for key, value in yaml_data.items():
        workflow = key
        globals()[workflow] = value
    if globals()[workflow]['Execution'] == "Sequential":
        execute_flow_serial(workflow, globals()[workflow]['Activities'])
    elif globals()[workflow]['Execution'] == "Concurrent":
        execute_flow_parallel(workflow, globals()[workflow]['Activities'])
