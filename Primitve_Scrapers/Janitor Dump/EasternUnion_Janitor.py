import openpyxl
import re

wb = openpyxl.load_workbook('C:\\Users\\James\\Desktop\\EasternUnion\\EasternUnion_Editable.xlsx')  # Change directory
sheet = wb.get_sheet_by_name('EasternUnion-1-Raw')
name_column = tuple(sheet['B:B'])
n = 0

tofind = re.compile(r"[a-z][A-Z]")
irish = re.compile(r"Mc")

for cell in name_column:
    cell_text = str(cell.value)
    if not tofind.findall(cell_text):
        continue
    else:
        for match in tofind.findall(cell_text):
            if irish.findall(cell_text) and match[0] == 'c':
                continue
            else:
                cell_text = re.sub(match, '{} {}'.format(match[0], match[1]), cell_text)
                cell.value = cell_text
                print('Reformatted {}'.format(cell_text))
                n += 1

wb.save('C:\\Users\\James\\Desktop\\EasternUnion\\EasternUnion_Reformatted.xlsx')  # Change Directory
wb.close()
print('Done. {} edits made'.format(n))
