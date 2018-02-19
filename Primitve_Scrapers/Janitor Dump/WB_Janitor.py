import openpyxl

wb = openpyxl.load_workbook('C:\\Users\\James\\Downloads\\WD_Raw.xlsx')
sheet = wb.get_sheet_by_name(wb.get_sheet_names()[0])
city_list = tuple(sheet['H:H'])
state_list = tuple(sheet['I:I'])
blank_list = tuple(sheet['P:P'])

for newcell, city, state in zip(blank_list, city_list,  state_list):
    new_text = 'Walker & Dunlop ({}, {})'.format(city.value, state.value)
    newcell.value = new_text
    print('Assigned {}'.format(new_text))

wb.save('WD_Edited.xlsx')
wb.close()



