import requests
import bs4
import pandas
import re
import os

TOSCRAPE = ['http://www.naiglobal.com/members/team/nai-geis-realty-group-inc-philadelphia',
            'http://www.naiglobal.com/members/team/nai-capital-orange-county-irvine',
            'http://www.naiglobal.com/members/team/nai-mertz-corporation-philadelphia',
            'http://www.naiglobal.com/members/team/nai-long-island-melville',
            'http://www.naiglobal.com/members/team/nai-platform-albany',
            'http://www.naiglobal.com/members/team/nai-mertz-southampton',
            'http://www.naiglobal.com/members/team/nai-highland-llc-colorado-springs',
            'http://www.naiglobal.com/members/team/nai-mountain-commercial-avon',
            'http://www.naiglobal.com/members/team/nai-shames-makovsky-denver',
            'http://www.naiglobal.com/members/team/nai-partners',
            'http://www.naiglobal.com/members/team/nai-robert-lynn-dallas',
            'http://www.naiglobal.com/members/team/nai-san-antonio',
            'http://www.naiglobal.com/members/team/evo-real-estate-group-new-york',
            'http://www.naiglobal.com/members/team/nai-klnb-baltimore',
            'http://www.naiglobal.com/members/team/nai-klnb-columbia',
            'http://www.naiglobal.com/members/team/nai-michael-lanham',
            'http://www.naiglobal.com/members/team/nai-klnb-washington',
            ]

def webdl(url):

    print('Downloading...')
    r = requests.get(url)
    r.raise_for_status()
    return r


def serachpageparsing(url):

    # TODO add search page parsing to auto-generate TO Scrape list

    return None


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

    employees = []

    for link in TOSCRAPE:
        page = webdl(link)

        new_list = htmlparsing(page)
        employees += new_list

        print("Updated NAI with %s" % os.path.basename(link))

    data_frame = pandas.DataFrame.from_records(employees)
    data_frame.to_csv('NAI_2.csv')
    print('Done')


if __name__ == "__main__":
    main()

# TODO Add Threading/Queuing functionality
