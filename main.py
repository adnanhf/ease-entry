import pandas
import reader
import settler
import wrapped


dfspb = reader.read_file('materials/SPB.IDBYQ.01.2024.xls')
dflkk = reader.read_file('materials/LK3.IDBYQ.2024-01.xls')
targets = reader.load_json('materials/dicts.json')

target_spb, target_lkk = targets['FSPB'], targets['FLKK']
combined_set, output_header = targets['COMBINED_SET'], targets['OUTPUT_HEADER']

materials = settler.pull_data([target_spb, target_lkk], [dfspb, dflkk])
sorted_materials = wrapped.sorted_dictionary(materials)
result = wrapped.combine_entries(sorted_materials, combined_set)

result = pandas.DataFrame(result)
result.to_excel('materials/Entries.xlsx')
