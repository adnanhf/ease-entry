import pandas
import json
import numpy


def read_file(filename='None'):
    dffile = pandas.read_excel(filename)
    dffile['Tanggal Tiba'] = dffile['Tanggal Tiba'].astype(str)
    dffile['Tanggal Tolak'] = dffile['Tanggal Tolak'].astype(str)

    return dffile


def load_json(filename='None'):
    file = open(filename)
    loadfile = json.load(file)

    return loadfile


def temp_storage(dataframe, target_column):
    temp = {}
    for item in target_column:
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
    consecutives = numpy.split(index_list, numpy.where(
        numpy.diff(index_list) != step_size)[0] + 1)

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


def element_combiner(elements, the_index, iterator):
    for item in iterator:
        if isinstance(elements[item][the_index], list):
            elements[item][the_index] = ';'.join(
                map(str, elements[item][the_index]))

    return elements


def assign_subdata(handled_data, index_list, target_keys):
    new_row_arrived = {key: [] for key in target_keys[:5]}
    new_row_departed = {key: [] for key in target_keys[5:]}

    for item in target_keys:
        new_arrived, new_departed = new_item_appender(
            handled_data, [item, index_list, target_keys])

        new_arrived = availability_checked(new_arrived)
        new_departed = availability_checked(new_departed)

        collections = new_row_arrived, new_row_departed, new_arrived, new_departed

        new_row_arrived, new_row_departed = new_row_appender(
            collections, [item, target_keys])

    for item in target_keys:
        if item in target_keys[:5]:
            handled_data[item][index_list[0]] = new_row_arrived[item][0]
        elif item in target_keys[5:]:
            handled_data[item][index_list[0]] = new_row_departed[item][0]

    handled_data = element_combiner(handled_data, index_list[0], target_keys)

    return handled_data


def handle_loads(handled, target):
    indices = return_indexes(handled['SPB'])
    for index_list in indices:
        refined = assign_subdata(handled, index_list, target)

    return refined


def pull_data(target, source):
    target01, target02 = target
    source01, source02 = source

    ships_info = temp_storage(source01, target01)
    loads_info = temp_storage(source02, target02)
    loads_info = handle_loads(loads_info, target02[1:])

    return ships_info, loads_info


def sorted_dictionary(given_dict):
    dict_01, dict_02 = given_dict

    dfclearance = pandas.DataFrame(
        dict_01).sort_values(by='SPB', ascending=True)
    dfloadings = pandas.DataFrame(
        dict_02).sort_values(by='SPB', ascending=True)

    dfclearance = dfclearance.to_dict(orient='list')
    dfloadings = dfloadings.query('SPB != "-"')
    dfloadings = dfloadings.to_dict(orient='list')

    return dfclearance, dfloadings


def combine_entries(separated_data, header):
    clearance, loadings = separated_data
    n_empty_list = [0] * len(clearance['SPB'])
    written = {key: n_empty_list for key in header}

    for key in header:
        if key in list(clearance.keys()):
            written[key] = clearance[key]
        else:
            written[key] = loadings[key]

    return written


dfspb = read_file('materials/SPB.IDBYQ.01.2024.xls')
dflkk = read_file('materials/LK3.IDBYQ.2024-01.xls')
targets = load_json('materials/dicts.json')

target_spb, target_lkk = targets['FSPB'], targets['FLKK']
combined_set, output_header = targets['COMBINED_SET'], targets['OUTPUT_HEADER']

materials = pull_data([target_spb, target_lkk], [dfspb, dflkk])
sorted_materials = sorted_dictionary(materials)
result = combine_entries(sorted_materials, combined_set)

result = pandas.DataFrame(result)
result.to_excel('materials/Entries.xlsx')
