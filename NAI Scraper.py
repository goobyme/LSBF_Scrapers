import requests
import bs4
import pandas
import re
import os

TOSCRAPE = 'http://www2.naicapital.com/offices/team/nai-capital-westlake-village'

def webdl(url):

    print('Downloading...')
    r = requests.get(url)
    r.raise_for_status()
    return r


def htmlparsing(page):

    locregex = re.compile(r"(?<=Location: ).+(?=\n)")
    specregex = re.compile(r"(?<=Specialties: ).+(?=\n)")
    phoneregex = re.compile(r"(?<=Phone: \r\n).+(?=\r\n)")
    titleregex = re.compile(r"(?<=Title: ).+(?=\n)")

    es = []
    html = bs4.BeautifulSoup(page.text, 'lxml')
    elements = html.find_all('div', {'class': 'tencol agentNTEPcontainer last'})

    for element in elements:
        e = {}
        name = element.find('h3').text
        e["Name"] = name.replace('\n', '')

        text = element.get_text()

        loc = locregex.findall(text)
        e["Location"] = loc[0]

        title = titleregex.findall(text)
        if title:
            e['Title'] = title[0]

        phone = phoneregex.findall(text)
        e["Phone"] = phone[0].replace(' ', '')

        email = element.find_all('a', {'class': 'tPageEmail'})
        em = email[0].get_text()
        e["Email"] = em

        specs = specregex.findall(text)
        if specs:
            e["Specialties"] = specs[0].strip()

        es.append(e)

    return es


def main():

    page = webdl(TOSCRAPE)

    employees = htmlparsing(page)

    file_name = os.path.basename(TOSCRAPE)
    data_frame = pandas.DataFrame.from_records(employees)
    data_frame.to_csv('NAI_%s.csv' % file_name)
    print('Done')


if __name__ == "__main__":
    main()
