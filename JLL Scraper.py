import requests
import bs4
import pandas
import re
import threading

# TODO REDO ALL IN SELENIUM, WILL BE BASIS FOR FUTURE SELENIUM BUILDS
#
#
#

SEARCHPAGE = 'http://www.us.jll.com/united-states/en-us/people#k=#s='
THREADCOUNT = 10
employees = []
list_lock = threading.Lock()


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


def personparsing(page):       #TODO test out bs4 selectors and then ur golden!

    es = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    table = soup.find_all('tbody')
    elements = table[0].find_all('tr')

    for element in elements:
        e = {}

        name = element.find_all('a')[0]
        e["Name"] = name['title']

        email = element.find_all('a')[1]
        em = email.get_text()
        e["Email"] = em

        loc_parent = element.find_all('td')
        loc = loc_parent[len(loc_parent) - 1]
        e["Location"] = loc.get_text

        title = element.find_all('td')
        e['Title'] = title[1]

        phone = element.find_all('td')
        e["Phone"] = phone[2].get_text

        specs = element.find_all('small')
        if specs:
            e["Specialties"] = specs[0].get_text

        es.append(e)

    return es


def threadbot(start, stop, id):

    sublist = []
    for s in range(start, stop, 10):
        page = webdl(SEARCHPAGE + '%s' %s)
        sublist += personparsing(page)
        print('Thread %s check' % id)
    print('Thread %s downloads complete' % id)

    list_lock.aquire()
    try:
      global employees
      employees += sublist
      print('Thread %s finished writing to list' % id)
    finally:
        list_lock.release()


def main():

    global employees
    threads = []
    interval = int((3354-(3354 % THREADCOUNT))/THREADCOUNT)

    for i in range(1, THREADCOUNT):
        if i != THREADCOUNT:
            thread = threading.Thread(target=threadbot(), args=[1+(i-1)*interval, i*interval, i])
        else:
            thread = threading.Thread(target=threadbot(), args=[1+(i-1) * interval, 3354, i])

        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    data_frame = pandas.DataFrame.from_records(employees)
    data_frame.to_csv('JLL.csv')
    print('Done')


# if __name__ == "__main__":
#     main()

page = webdl(SEARCHPAGE)
print(personparsing(page))

# TODO Chang person parsing to look at indv. profile page and id data from description paragraphs (not sure if possible)
# TODO Improve error handling
