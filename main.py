import pandas as pd
import json


def read_file(filename='None'):
    dffile = pd.read_excel(filename)

    return dffile


def load_json(filename='None'):
    file = open(filename)
    loadfile = json.load(file)

    return loadfile


dfspb = read_file('materials/SPB.IDBYQ.12.2023.xls')
dflkd = read_file('materials/LK3.DN.IDBYQ.2023-12-01.xls')
dflke = read_file('materials/LK3.LN.IDBYQ.2023-12-01.xls')

output_dict = load_json('json/output_dict.json')
target = load_json('json/targets.json')
target_spb, target_lkk = target['FSPB'], target['FLKK']
