import numpy

def temp_storage(dataframe, target_column):
    temp = {}
    for item in target_column:
        temp[item] = dataframe[item].tolist()

    return temp


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


def new_item_appender(given_data, validators):
    list_01, list_02 = [], []
    the_key, index_list, marked_keys = validators

    for idx in index_list:
        if the_key in marked_keys[:5]:
            list_01.append(given_data[the_key][idx])
        elif the_key in marked_keys[5:]:
            list_02.append(given_data[the_key][idx])

    return list_01, list_02


def new_row_appender(collections, validations):
    the_key, marked_keys = validations
    dict_01, dict_02, list_01, list_02 = collections

    if the_key in marked_keys[:5]:
        dict_01[the_key].append(list_01)
    elif the_key in marked_keys[5:]:
        dict_02[the_key].append(list_02)

    return dict_01, dict_02


def element_combiner(elements, the_index, iterator):
    for item in iterator:
        if isinstance(elements[item][the_index], list):
            elements[item][the_index] = ';'.join(
                map(str, elements[item][the_index]))

    return elements


def handle_loads(handled, target):
    indices = return_indexes(handled['SPB'])
    for index_list in indices:
        refined = assign_subdata(handled, index_list, target)

    return refined


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
            handled_data[item][index_list[0]] = new_row_arrived[item][0]
        elif item in target_keys[5:]:
            handled_data[item][index_list[0]] = new_row_departed[item][0]

    handled_data = element_combiner(handled_data, index_list[0], target_keys)

    return handled_data


def pull_data(target, source):
    target01, target02 = target
    source01, source02 = source

    ships_info = temp_storage(source01, target01)
    loads_info = temp_storage(source02, target02)
    loads_info = handle_loads(loads_info, target02[1:])

    return ships_info, loads_info