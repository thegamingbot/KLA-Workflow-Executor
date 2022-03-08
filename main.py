from datetime import datetime
import re
from time import sleep
import yaml
import pandas as pd
import logging
import operator
from concurrent.futures import ThreadPoolExecutor

ops = {'<': operator.lt, '>': operator.gt,
       '<=': operator.le, '>=': operator.ge}


def parse_yaml(file_name):
    """Function to parse yaml file"""
    with open(file_name) as f:
        data = yaml.safe_load(f)
    return data


def parse_csv(file_name):
    """Function to parse csv file"""
    df = pd.read_csv(file_name)
    return df


def parse_txt(file_name):
    """Function to parse txt file"""
    with open(file_name) as f:
        data = f.read()
    return data


def TimeFunction(name, inp):
    if inp['FunctionInput'][0] == '$':
        var = re.findall(r'\(.*?\)', inp['FunctionInput'])[0][1:-1]
        while True:
            try:
                fun_inp = globals()[var]
                break
            except:
                pass
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
        f"{datetime.now()};{name} Executing Binning ({inp['RuleFilename']})")
    bin_data = parse_csv(inp['RuleFilename'])
    df = globals()[inp['DataSet'][2:-1]].copy()
    bin_data['RULE'] = bin_data['RULE'].apply(lambda x: x.split(' and '))
    rule = [i.split(' ') for i in bin_data['RULE'][0]]
    bin_code = []
    for index, i in df.iterrows():
        cond = False
        for j in rule:
            if ops[j[1]](int(i['Signal']), int(j[2])):
                cond = True
            else:
                cond = False
                break
        if cond:
            bin_code.append(bin_data.iloc[0]['BIN_ID'])
        else:
            bin_code.append(0)
    df['Bincode'] = bin_code
    return 'bin', df, df.shape[0]


def MergeResults(name, inp):
    logging.warning(
        f"{datetime.now()};{name} Executing Merging ({inp['PrecedenceFile']})")
    precedence = parse_txt(inp['PrecedenceFile']).split(' >> ')
    df1 = globals()[inp['DataSet1'][2:-1]]
    df2 = globals()[inp['DataSet2'][2:-1]]
    df3 = globals()[inp['DataSet3'][2:-1]]
    df4 = globals()[inp['DataSet4'][2:-1]]
    df5 = globals()[inp['DataSet5'][2:-1]]
    df = df1[['Id', 'X', 'Y', 'Signal']].copy()
    final_bin_code = []
    for i in range(df.shape[0]):
        bin_codes = [df1.iloc[i]['Bincode'], df2.iloc[i]['Bincode'], df3.iloc[i]['Bincode'],
                     df4.iloc[i]['Bincode'], df5.iloc[i]['Bincode']]
        bin_code = max(bin_codes)
        for j in bin_codes:
            if j in precedence:
                if precedence.index(j) < precedence.index(bin_code):
                    bin_code = j
        final_bin_code.append(bin_code)
    df['Bincode'] = final_bin_code
    return 'merge', df, df.shape[0]


def ExportResults(name, inp):
    logging.warning(
        f"{datetime.now()};{name} Executing Exporting ({inp['FileName']})")
    df = globals()[inp['DefectTable'][2:-1]]
    df.to_csv(inp['FileName'], index=False)


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
    logging.warning(f"{datetime.now()};{name} Entry")
    if 'Condition' in data:
        var = re.findall(r'\(.*?\)', data['Condition'])[0][1:-1]
        sym = data['Condition'].split(var)[1].split(' ')[1]
        val = int(data['Condition'].split(var)[1].split(' ')[2])
        while True:
            try:
                x = globals()[var]
                if ops[sym](x, val):
                    return_data = globals()[data['Function']](
                        name, data['Inputs'])
                else:
                    return_data = None
                    logging.warning(f"{datetime.now()};{name} Skipped")
                break
            except:
                continue
    else:
        return_data = globals()[data['Function']](name, data['Inputs'])
    if return_data:
        if return_data[0] == "load":
            globals()[f'{name}.DataTable'] = return_data[1]
            globals()[f'{name}.NoOfDefects'] = return_data[2]
        elif return_data[0] == "bin":
            globals()[f'{name}.BinningResultsTable'] = return_data[1]
            globals()[f'{name}.NoOfDefects'] = return_data[2]
        elif return_data[0] == "merge":
            globals()[f'{name}.MergedResults'] = return_data[1]
            globals()[f'{name}.NoOfDefects'] = return_data[2]
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
