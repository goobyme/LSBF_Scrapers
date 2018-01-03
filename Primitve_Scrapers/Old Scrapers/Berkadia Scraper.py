import requests
import bs4
import pandas
import re
import threading

import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

SEARCHPAGE = 'https://www.berkadia.com/people-and-locations/people/'
THREADCOUNT = 10
employees = []
init_proto_profile_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()


def webdl(url):
    """Downloads web-page (using requests rather than urllib) """
    print('Downloading...%s' % url)
    r = requests.get(url, verify=False)
    try:
        r.raise_for_status()
        return r
    except requests.HTTPError:
        print('Download failed for %s' % url)
        return None


def searchpageparsing(page):
    """Scrapes search page for team links by location (can expand to search NAI globally)"""
    scrapelist = []

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_elements = soup.find_all('div', {'class': 'col-xs-3 col-sm-3 col-md-2'})

    for element in parent_elements:
        link_element = element.find('a')
        link = link_element['href']

        try:
            scrapelist.append(link)
        except IndexError:
            print('Search-page link parsing failed')

    return scrapelist


def personparsing(page):

    es = []
    element = bs4.BeautifulSoup(page.text, 'lxml')

    phoneregex = re.compile(r"(?<=Phone: ).+(?=\n)")
    faxregex = re.compile(r"(?<=Fax: ).+(?=\n)")

    e = {}

    name = element.find_all('h1')[0]
    e["Name"] = name.get_text()

    title = element.find_all('h2')[0]
    e['Title'] = title.get_text()

    email_parent = element.find_all('span', {'class': 'email'})
    email = email_parent[0].find_all('a')
    e["Email"] = email[0].get_text()

    loc_parent = element.find_all('div', {'class': 'primary-location'})
    loc = loc_parent[0].find_all('a')
    location = loc[0].get_text()
    e["Location"] = location.strip()

    phone_parent = element.find_all('div', {'class': 'locations'})
    phone_text = phone_parent[0].get_text()
    phone = phoneregex.findall(phone_text)
    try:
        e["Phone"] = phone[0]
    except IndexError:
        pass

    fax = faxregex.findall(phone_text)
    try:
        e['Fax'] = fax[0]
    except IndexError:
        pass

    details = element.find_all('div', {'class': 'detail-column'})

    if details:
        edu_elements = details[0].find_all('li')
        education = ''
        for i in edu_elements:
            part = i.get_text()
            education += part + ', '
        e['Education'] = education

        if len(details) > 1:
            aff_elements = details[1].find_all('li')
            aff = ''
            for i in aff_elements:
                part = i.get_text()
                aff += part + ', '
            e['Affiliations'] = aff.replace('\n', '')

    es.append(e)
    return es


def threadbot(thread_id):

    sublist = []
    while True:
        llist_lock.acquire()
        if len(init_proto_profile_list) > 0:
            try:
                link = init_proto_profile_list[0]
                init_proto_profile_list.remove(link)
            finally:
                llist_lock.release()
            print('Thread %s parsing %s' % (thread_id, link))
            sublist += personparsing(webdl(link))
        else:
            llist_lock.release()
            print('Thread %s completed parsing' % thread_id)
            break

    elist_lock.acquire()
    try:
        global employees
        employees += sublist
        print('Thread %s wrote to list' % thread_id)
    finally:
        elist_lock.release()


def main():

    global employees
    global init_proto_profile_list
    link_list = searchpageparsing(webdl(SEARCHPAGE))
    threads = []

    for i in range(THREADCOUNT):
        thread = threading.Thread(target=threadbot, args=(i+1, ))

        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    data_frame = pandas.DataFrame.from_records(employees)
    data_frame.to_csv('Berkadia.csv')
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Add progress checking capabilities
# TODO Improve error handling
