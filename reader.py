import pandas
import json


def read_file(filename='None'):
    dffile = pandas.read_excel(filename)
    dffile['Tanggal Tiba'] = dffile['Tanggal Tiba'].astype(str)
    dffile['Tanggal Tolak'] = dffile['Tanggal Tolak'].astype(str)

    return dffile


def load_json(filename='None'):
    file = open(filename)
    loadfile = json.load(file)

    return loadfile