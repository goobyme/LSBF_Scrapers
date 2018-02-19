import bs4
import pandas
import re


def parser(raw_text):
    employee_list = list()
    soup = bs4.BeautifulSoup(raw_text, 'lxml')
    cityregex = re.compile(r"^.+(?=,)")
    stateregex = re.compile(r"[A-Z]{2}")
    zipregex = re.compile(r"\d{5}$")

    table_el = soup.find('table', {'class':'tablesorter'} )
    for profile_el in table_el.find_all('tr'):
        e = dict()
        for number, item_el in enumerate(profile_el.find_all('td')):
            if number == 0:
                try:
                    raw_email = item_el.find('a')['href']
                    e['Email'] = raw_email.repace('mailto:', '')
                except AttributeError:
                    continue
            if number == 1:
                e['FullName'] = item_el.text
            elif number == 2:
                e['Designations'] = item_el.text
            elif number == 3:
                raw_address_text = str(item_el)
                stripped_text = raw_address_text.strip('<td>').strip('</td>')
                split_text = stripped_text.split('<br/>')
                for sub_number, text in enumerate(split_text):
                    if sub_number == 0:
                        e['Company'] = text
                    elif sub_number == 1:
                        e['StreetAddress'] = text
                    elif sub_number == 2:
                        try:
                            e['City'] = cityregex.findall(text)[0]
                            e['State'] = stateregex.findall(text)[0]
                            e['PostalCode'] = zipregex.findall(text)[0]
                        except IndexError:
                            continue

            elif number == 4:
                e['Phone'] = item_el.text
            elif number == 5:
                e['Fax'] = item_el.text
            elif number == 6:
                e['LocationServed'] = item_el.text
            elif number == 7:
                e['Website'] = item_el.text

        employee_list.append(e)

    return employee_list


def main():
    with open('C:\\Users\\James\\PycharmProjects\\LSBF_Scrapers\\Primitve_Scrapers\\nyscar_html.txt', 'r') as html:
        raw_text = html.read()
        employees = parser(raw_text)

    to_save = filter(None, employees)   # Gets rid of NoneTypes in list (very annoying if you don't do this!)
    data_frame = pandas.DataFrame.from_records(to_save)
    while True:
        try:
            data_frame.to_csv('NYSCAR.csv')
            break
        except PermissionError:
            print('Please close csv file')
            input()
    print('Done')


if __name__ == "__main__":
    main()
