import requests
import bs4
import pandas
import re
import os
import threading

SEARCHPAGE = 'http://www.naiglobal.com/about-nai'


def webdl(url):
    """Downloads web-page (using requests rather than urllib) """
    print('Downloading...')
    r = requests.get(url)
    try:
        r.raise_for_status()
        return r
    except requests.HTTPError:
        print('Download failed for %s' % url)
        return None


def searchpageparsing(page):
    """Scrapes search page for team links by location (can expand to search NAI globally)"""
    scrapelist = []
    linkregex = re.compile(r"(?<=/members/).+")

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_element = soup.find_all('ul', {'region_name': 'North America'})
    sub_elements = parent_element[0].find_all('ul', {'class': 'regionLocations'})
    link_elements = sub_elements[1].find_all('a', {'class': 'jt'})  # Note that [1] refers to USA only excluding Canada

    for link in link_elements:

        link_format = linkregex.findall(link['href'])
        try:
            link_final = 'http://www.naiglobal.com/members/team/' + link_format[0]
            scrapelist.append(link_final)
        except IndexError:
            print('Search-page link parsing failed')

    return scrapelist


def htmlparsing(page):

    locregex = re.compile(r"(?<=Location: ).+(?=\n)")
    specregex = re.compile(r"(?<=Specialties: ).+(?=\n)")
    phoneregex = re.compile(r"(?<=Phone: \r\n).+(?=\r\n)")
    titleregex = re.compile(r"(?<=Title: ).+(?=\n)")

    es = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    elements = soup.find_all('div', {'class': 'tencol agentNTEPcontainer last'})

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
    search = webdl(SEARCHPAGE)

    for link in searchpageparsing(search):
        page = webdl(link)
        if page:
            new_list = htmlparsing(page)
            employees += new_list
            print("Updated NAI with %s" % os.path.basename(link))
        else:
            continue

    data_frame = pandas.DataFrame.from_records(employees)
    data_frame.to_csv('NAI_2.csv')
    print('Done')


if __name__ == "__main__":
    main()

# TODO Add Threading/Queuing functionality via integration into M&M search tool
# TODO Chang person parsing to look at indv. profile page and id data from description paragraphs
# TODO Improve error handling
