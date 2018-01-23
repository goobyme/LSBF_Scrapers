import requests
import bs4
import pandas
import re
import threading
import os
import logging
import datetime
from Primitve_Scrapers.Reference import StateCodesList
import json

"""Globals and Setup"""
THREADCOUNT = 20
SEARCHPAGE = 'INSERT URL HERE IF STARTPAGE IS NEEDED'

employees = []
init_proto_profile_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()
err_lock = threading.Lock()


def webdl(url, retries=3, threadident=0):
    """Downloads web-page, retries on initial failures, returns None if fails thrice (common!)"""
    print('Downloading...{}'.format(url))
    for i in range(retries):
        try:
            r = requests.get(url)
            r.raise_for_status()
            return r
        except Exception:     # Ugly but it works, hard to account for all the possible error codes otherwise
            print('[Warning-{} webdl]: Retrying Download'.format(threadident))
            continue
    error_writer('webdl', 'Download failed for {}'.format(url), threadident)
    return None


def error_writer(func, message, thread):
    """Prints out error code and writes to error-dump file"""
    if not os.path.isfile('errordump.txt'):
        f = open('errordump.txt', 'w')
        f.close()

    fullmessage = '\n[Error-{} {}]: {}  {}\n'.format(thread, func, message, datetime.datetime.now())
    print(fullmessage)
    err_lock.acquire()
    try:
        with open('errordump.txt', 'a') as error_dump:
            error_dump.write(fullmessage.strip())
    finally:
        err_lock.release()


def employeelistparsing(page, thread_ident):
    """Parses each indv. profile listing page to return proto-profiles with info from search-page"""
    proto_profiles = []
    html_str = json.loads(page.text)['list']
    soup = bs4.BeautifulSoup(html_str, 'lxml')

    for parent in soup.find_all('h4'):
        proto = {
            'Link': 'https://commercial.century21.com' + parent.find('a')['href'],
            'FullName': parent.find('a').get_text(),
        }
        proto_profiles.append(proto)
    return proto_profiles


def personparsing(page, thread_ident, profile):
    """Parses from some combination of vcf and page and outputs json (to be iterated outside of func)"""
    try:
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return profile
    e = profile
    try:
        e['WorkPhone'] = soup.find('a', {'data-ctc-track': '["agent-ADP-CTC","call-agent-phone"]'}).get_text()
    except AttributeError:
        pass
    try:
        e['MobilePhone'] = soup.find('a', {'data-ctc-track': '["agent-ADP-CTC","call-agent-mobile"]'}).get_text()
    except AttributeError:
        pass
    e['Link'] = soup.find('a', {'class': 'moreFromAgent'})['href']


    """Languages, Awards, Professional Designations"""
    regex = re.compile(r"\s{2,}")
    for i in range(soup.find_all('div', {'class': 'designationTxt'})):
        el = soup.find_all('div', {'class': 'designationTxt'})[i]
        title = soup.find_all('h4')[i]
        e[title] = regex.sub(', ', el.get_text())

    """Department/Office and Address"""
    streetregex = re.compile(r"^.+,")
    cityregex = re.compile(r"(?<=, ).+(?=,)")
    stateregex = re.compile(r"[A-Z]{2}")
    zipregex = re.compile(r"\d{5}$")

    parent = soup.find('h3', {'class': 'officeName'})
    e['Department/Office'] = parent.find('b').get_text()
    parse_text = parent.replace(e['Department/Office'] + ' ', '')
    try:
        e['StreetAddress'] = streetregex.findall(parse_text)[0]
    except IndexError:
        pass
    try:
        e['City'] = cityregex.findall(parse_text)[0]
    except IndexError:
        pass
    try:
        e['State'] = stateregex.findall(parse_text)[0]
    except IndexError:
        pass
    try:
        e['PostalCode'] = zipregex.findall(parse_text)[0]
    except IndexError:
        pass

    """Property Type"""
    property_types = []
    for el in soup.find_all('div', {'class': 'listingType truncate'}):
        if el.find('b').get_text() not in property_types:
            property_types.append(el.find('b').get_text())
        else:
            continue
    e['PropertyTypes'] = property_types

    """Areas Serviced"""
    locations = []
    parent = soup.find('div', {'class': 'locationWrapper'})
    for el in parent.find_all('a'):
        if el.get_text() not in locations:
            locations.append(el.get_text)
    e['AreasServiced'] = locations

    return e


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses to generate profile sublists , then merges with master"""
    print('Threadbot {} Initialized'.format(ident))
    sublist = []

    """Iterated function over global list with hand-coded queueing"""
    # TODO integrate built in Queue functionality into threadbot
    while True:
        llist_lock.acquire()
        if len(init_proto_profile_list) > 0:
            try:
                link = init_proto_profile_list[0]    # Take proto-profile from list and removes from queue
                init_proto_profile_list.remove(link)
                length = len(init_proto_profile_list)
            finally:
                llist_lock.release()
            print('Thread {} parsing link {} of {}'.format(ident, total_len - length, total_len))

            """From here either feed through employeepageparsing or directly through personparsing"""
            sublink = link
            pagenumberregex = re.compile(r"(?<=&s=)\d+")
            while True:
                protoprofiles = employeelistparsing(webdl(sublink, ident), ident)
                if protoprofiles:
                    for prof in protoprofiles:
                        full_prof = personparsing(webdl(prof['Link'], ident), ident, prof)
                        sublist.append(full_prof)
                    pagenumberregex.sub(sublink, str(int(pagenumberregex.findall(sublink)[0]) + 10))
                else:
                    break
        else:
            llist_lock.release()
            print('Thread {} completed parsing'.format(ident))
            break

    """Final merge with global list"""
    print('Thread {} writing to list...'.format(ident))
    elist_lock.acquire()
    try:
        global employees
        employees += sublist
    finally:
        elist_lock.release()


def main():
    global employees
    global init_proto_profile_list
    for state in StateCodesList.codes:
        init_proto_profile_list.append(
            'https://commercial.century21.com/search.c21?lid=S{}&t=2&o=&s=0&subView=searchView.Paginate'.format(state))
    startlength = len(init_proto_profile_list)
    threads = []

    for i in range(THREADCOUNT):
        thread = threading.Thread(target=threadbot, args=(i+1, startlength, ))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    to_save = filter(None, employees)   # Gets rid of NoneTypes in list (very annoying if you don't do this!)
    data_frame = pandas.DataFrame.from_records(to_save)
    while True:
        try:
            data_frame.to_csv('Century21.csv')
            break
        except PermissionError:
            print('Please close csv file')
            input()
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Add Time checking capabilities
# TODO Create an Error Dump for self-warning
