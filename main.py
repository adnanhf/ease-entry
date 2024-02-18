import pandas
import json
import numpy
import csv


def read_file(filename='None'):
    dffile = pandas.read_excel(filename)

    return dffile


def load_json(filename='None'):
    file = open(filename)
    loadfile = json.load(file)

    return loadfile


def temp_storage(dataframe, target_column):
    temp = {}
    for item in target_column:
        dataframe[item] = dataframe[item].fillna(0)
        temp[item] = dataframe[item].tolist()

    return temp


def count_nonzero(data_list):
    counts = 0
    for item in data_list:
        if item != 0:
            counts += 1

    return counts


def availability_checked(given_list):
    if any(given_list) != 0:
        new_list = [element for element in given_list if element != 0]
    else:
        new_list = 0

    return new_list


def return_indexes(handled_data, step_size=1):
    index_list = [idx for idx, item in enumerate(handled_data) if item == '-']
    consecutives = numpy.split(index_list, numpy.where(numpy.diff(index_list) != step_size)[0] + 1)
    
    for idx, sublist in enumerate(consecutives):
        sublist = sublist.tolist()
        sublist.insert(0, sublist[0]-1)
        consecutives[idx] = sublist

    return consecutives


def new_row_appender(collections, validations):
    the_key, marked_keys = validations
    dict_01, dict_02, list_01, list_02 = collections

    if the_key in marked_keys[:5]:
        dict_01[the_key].append(list_01)
    elif the_key in marked_keys[5:]:
        dict_02[the_key].append(list_02)

    return dict_01, dict_02


def new_item_appender(given_data, validators):
    list_01, list_02 = [], []
    the_key, index_list, marked_keys = validators

    for idx in index_list:
        if the_key in marked_keys[:5]:
            list_01.append(given_data[the_key][idx])
        elif the_key in marked_keys[5:]:
            list_02.append(given_data[the_key][idx])

    return list_01, list_02

def assign_subdata(handled_data, index_list, target_keys):
    new_row_arrived = {key: [] for key in target_keys[:5]}
    new_row_departed = {key: [] for key in target_keys[5:]}

    for item in target_keys:
        new_arrived, new_departed = new_item_appender(handled_data, [item, index_list, target_keys])

        new_arrived = availability_checked(new_arrived)
        new_departed = availability_checked(new_departed)

        collections = new_row_arrived, new_row_departed, new_arrived, new_departed

        new_row_arrived, new_row_departed = new_row_appender(collections, [item, target_keys])

    for item in target_keys:
        if item in target_keys[:5]:
            handled_data[item][index_list[0]] = new_row_arrived[item]
        elif item in target_keys[5:]:
            handled_data[item][index_list[0]] = new_row_departed[item]

    return handled_data


def handle_loads(handled, target):
    indices = return_indexes(handled['SPB'])
    for index_list in indices:
        refined = assign_subdata(handled, index_list, target)

    #print (refined['Komoditi B'])

    return refined


def pull_data(target, source):
    ships_info = {}
    target01, target02 = target
    source01, source02 = source

    for item in target01:
        ships_info[item] = source01[item].tolist()
    loads_info = temp_storage(source02, target02)

    #print(loads_info['Komoditi B'])

    loads_info = handle_loads(loads_info, target02[1:])

    #print(loads_info['Komoditi M'])

    return ships_info, loads_info


def combine_entries(separated_data, header):
    clearance, loadings = separated_data
    n_empty_list = [0] * len(clearance['SPB'])
    written = {key: n_empty_list for key in header}

    for key in clearance:
        written[key] = clearance[key]
    
    loading_keys = list(loadings.keys())
    loading_keys = loading_keys

    for key in loading_keys:
        for idx, id in enumerate(clearance['SPB']):
            written[key][idx] = loadings[key][loadings['SPB'].index(id)]
            print(written[key][idx], loadings[key][loadings['SPB'].index(id)], loadings['SPB'][loadings['SPB'].index(id)])
        
    print(written['Komoditi M'])

    return written


dfspb = read_file('materials/SPB.IDBYQ.12.2023.xls')
dflkk = read_file('materials/LK3.IDBYQ.2023-12-01.xls')
targets = load_json('materials/dicts.json')

target_spb, target_lkk = targets['FSPB'], targets['FLKK']
combined_set, output_header = targets['COMBINED_SET'], targets['OUTPUT_HEADER']

materials = pull_data([target_spb, target_lkk], [dfspb, dflkk])
result = combine_entries(materials, combined_set)

result = pandas.DataFrame(result)
result.to_excel('materials/Entries.xlsx')
