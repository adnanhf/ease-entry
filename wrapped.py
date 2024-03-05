import pandas

def sorted_dictionary(given_dict):
    dict_01, dict_02 = given_dict

    dfclearance = pandas.DataFrame(dict_01).sort_values(by='SPB', ascending=True)
    dfloadings = pandas.DataFrame(dict_02).sort_values(by='SPB', ascending=True)

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