import csv
import pandas

employees = []
firms = []

with open('C:\\Users\\James\\PycharmProjects\\LSBF_Scrapers\\Primitve_Scrapers\\Janitor Dump\\nadcoemail.csv',
          newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        if row.setdefault('Type', 'Employee') == 'Employee':
            employees.append(row)
        else:
            firms.append(row)
    csvfile.close()


def exporter_to_csv(filename, dic_list):
    dataframe = pandas.DataFrame.from_records(dic_list)
    dataframe.to_csv(filename)
    print('Exported {}'.format(filename))

exporter_to_csv('nadco_employees.csv', employees)
exporter_to_csv('nadco_firms.csv', firms)
