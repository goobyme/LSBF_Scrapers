import requests, os, bs4, pandas


def webdl(url):

    print('Downloading...')
    r = requests.get(url)
    r.raise_for_status()
    return r


def htmlparsing(page):

    es = []
    html = bs4.BeautifulSoup(page.text, 'lxml')
    elements = html.find_all('div', {'class': 'tencol agentNTEPcontainer last'})

    for element in elements:
        e = {}
        name = element.find('h3').text
        e["Name"] = name

        text = element.get_text()

        loc = element.find('b').text
        e["Location"] = loc

        # phone = element.select
        # e["Phone"] = phone
        #
        email = element.find('a', href)
        e["Email"] = email

        # specs = element.select
        # e["Speciality"] = specs

        es.append(e)

    return es

def main():

    url = input()
    page = webdl(url)

    employees = htmlparsing(page)

    data_frame = pandas.DataFrame.from_records(employees)
    data_frame.to_csv('NAI.csv')


page = webdl('http://www2.naicapital.com/offices/team/nai-capital-hq-encino')
enames = htmlparsing(page)
print(enames)
