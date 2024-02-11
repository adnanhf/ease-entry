import pandas
import json
import numpy


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


def return_indexes(handled_data, step_size=1):
    index_list = [idx for idx, item in enumerate(handled_data) if item == 0]
    consecutives = numpy.split(index_list, numpy.where(numpy.diff(index_list) != step_size)[0] + 1)
    for idx, sublist in enumerate(consecutives):
        sublist = sublist.tolist()
        sublist.insert(0, sublist[0]-1)
        consecutives[idx] = sublist

    return consecutives


def assign_subdata(handled_data, indices, target_keys):
    for sublist in indices:
        new_row_arrived: dict = {key: [] for key in target_keys[:5]}
        new_row_departed = {key: [] for key in target_keys[5:]}
        for item in target_keys:
            new_arrived, new_departed = [], []
            for idx in sublist:
                if item in target_keys[:5]:
                    new_arrived.append(handled_data[item][idx])
                elif item in target_keys[5:]:
                    new_departed.append(handled_data[item][idx])

            if any(new_arrived) != 0:
                new_arrived = [element for element in new_arrived if element != 0]
            else:
                new_arrived = 0

            if any(new_departed) != 0:
                new_departed = [element for element in new_departed if element != 0]
            else:
                new_departed = 0

            if item in target_keys[:5]:
                new_row_arrived[item].append(new_arrived)
            elif item in target_keys[5:]:
                new_row_departed[item].append(new_departed)

        for item in target_keys:
            if item in target_keys[:5]:
                handled_data[item][sublist[0]] = new_row_arrived[item]
            elif item in target_keys[5:]:
                handled_data[item][sublist[0]] = new_row_departed[item]

    return handled_data


def handle_loads(handled, target):
    indices = return_indexes(handled['SPB'])
    refined = assign_subdata(handled, indices, target)

    return refined


def pull_data(target, source):
    ships_info = {}
    target01, target02, target03 = target
    source01, source02 = source

    for item in target01:
        ships_info[item] = source01[item].tolist()
    loads_info = temp_storage(source02, target02)
    loads_info = handle_loads(loads_info, target03)

    return ships_info, loads_info


def write_entry(clearance, loads_data, header):
    written_entry = {}
    if count_nonzero(clearance['SPB']) == count_nonzero(loads_data['SPB']):
        for item in header:
            if item in clearance.keys() and item in loads_data.keys():
                written_entry[item] = clearance[item]
            # if item in loads_data.keys():
                # written_entry[item] = loads_data[item]


dfspb = read_file('materials/SPB.IDBYQ.12.2023.xls')
dflkk = read_file('materials/LK3.IDBYQ.2023-12-01.xls')

targets = load_json('json/targets.json')
target_spb, target_lkk, target_lkk_lite = targets['FSPB'], targets['FLKK'], targets['FLKK_LITE']
combined_set, output_header = targets['COMBINED_SET'], targets['OUTPUT_HEADER']

materials = pull_data([target_spb, target_lkk, target_lkk_lite], [dfspb, dflkk])
# write_entry(materials[0],materials[1], combined_set)
