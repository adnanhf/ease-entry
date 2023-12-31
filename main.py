import pandas as pd
import json


def read_file(filename='None'):
    dffile = pd.read_excel(filename)

    return dffile


def load_json(filename='None'):
    file = open(filename)
    loadfile = json.load(file)

    return loadfile


def pull_data(target, source):
    pulled = {}
    target01, target02 = target
    source01, source02 = source


dfspb = read_file('materials/SPB.IDBYQ.12.2023.xls')
dflkk = read_file('materials/LK3.IDBYQ.2023-12-01.xls')

targets = load_json('json/targets.json')
target_spb, target_lkk, OutputHeader = targets['FSPB'], targets['FLKK'], targets['OutputHeader']

pull_data([target_spb, target_lkk], [dfspb, dflkk])
