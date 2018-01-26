import requests
import bs4
import pandas
import re
import threading
import os
import datetime
import time
import json

StateCodeList = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
      "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
      "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
      "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
      "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

"""Globals and Setup"""
THREADCOUNT = 20
SEARCHPAGE = 'INSERT URL HERE IF STARTPAGE IS NEEDED'

employees = []
init_proto_profile_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()
err_lock = threading.Lock()


def webdl(url, threadident=0, retries=3):
    """Downloads web-page, retries on initial failures, returns None if fails thrice (common!)"""
    print('{}-Downloading...{}'.format(threadident, url))
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
    page.close()

    extraregex = re.compile(r"of.+")
    for parent in soup.find_all('h4'):
        proto = {
            'Link': 'https://commercial.century21.com' + parent.find('a')['href'],
            'FullName': extraregex.sub('', parent.find('a').get_text()),
        }
        proto_profiles.append(proto)
    return proto_profiles


def personparsing(page, thread_ident, profile):
    """Parses from some combination of vcf and page and outputs json (to be iterated outside of func)"""
    try:
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return profile
    finally:
        page.close()
    e = profile

    try:
        e['WorkPhone'] = soup.find('a', {'data-ctc-track': '["agent-ADP-CTC","call-agent-phone"]'}).get_text()
    except AttributeError:
        pass
    try:
        e['MobilePhone'] = soup.find('a', {'data-ctc-track': '["agent-ADP-CTC","call-agent-mobile"]'}).get_text()
    except AttributeError:
        pass

    """Languages, Awards, Professional Designations"""
    regex = re.compile(r"\s{2,}")
    try:
        for i in range(len(soup.select('div.designationTxt'))):
            if soup.select_one('h4').get_text() != 'Personal Profile':
                el = soup.select('div.designationTxt')[i]
                title = soup.select('h4')[i].get_text()
                e[title] = regex.sub(', ', el.get_text())
            else:
                el = soup.select('div.designationTxt')[i]
                title = soup.select('h4')[i+1].get_text()
                e[title] = regex.sub(', ', el.get_text())
    except TypeError or IndexError:
        pass

    """Department/Office and Address"""
    streetregex = re.compile(r"^.+,")
    cityregex = re.compile(r"(?<=, ).+(?=,)")
    stateregex = re.compile(r"[A-Z]{2}")
    zipregex = re.compile(r"\d{5}$")

    parent = soup.select_one('h3.officeName')
    e['Department/Office'] = parent.find('b').get_text()
    parse_text = parent.get_text().replace(e['Department/Office'] + ' ', '')
    try:
        e['City'] = cityregex.findall(parse_text)[0]
        try:
            e['StreetAddress'] = streetregex.findall(parse_text)[0].replace(e.get('City'), '')
        except IndexError:
            pass
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
    parent = soup.select_one('div.locationWrapper')
    if parent:
        for el in parent.find_all('a'):
            if el.get_text() not in locations:
                locations.append(el.get_text())
    e['AreasServiced'] = locations

    if soup.select_one('a.moreFromAgent'):
        e['ProfilePage'] = soup.select_one(('a.moreFromAgent'))['href']

    print('{}-Finished parsing {}'.format(thread_ident, e.get('Link')))

    return e


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses to generate profile sublists , then merges with master"""
    print('Threadbot {} Initialized'.format(ident))

    def writetoglobal(sublist):
        global employees
        print('Thread {} writing to list...'.format(ident))
        elist_lock.acquire()
        try:
            print('{}-Writing to global'.format(ident))
            employees += sublist
        finally:
            elist_lock.release()

    while True:
        llist_lock.acquire()
        if len(init_proto_profile_list) > 0:
            try:
                sublink = init_proto_profile_list[0]    # Take proto-profile from list and removes from queue
                init_proto_profile_list.remove(sublink)
                length = len(init_proto_profile_list)
            finally:
                llist_lock.release()
            print('Thread {} parsing link {} of {}'.format(ident, total_len - length, total_len))
            # From here either feed through employeepageparsing or directly through personparsing
            pagenumberregex = re.compile(r"(?<=&s=)\d+")
            while True:
                sublist = []
                a = True
                for i in range(2):
                    protoprofiles = employeelistparsing(webdl(sublink, ident), ident)
                    if protoprofiles:
                        for prof in protoprofiles:
                            sublist.append(personparsing(webdl(prof['Link'], ident), ident, prof))
                        sublink = pagenumberregex.sub(str(int(pagenumberregex.findall(sublink)[0]) + 10), sublink)
                    else:
                        writetoglobal(sublist)
                        a = False
                        break

                    if i == 1:
                        writetoglobal(sublist)
                    else:
                        continue

                if not a:
                    break
                else:
                    continue

        else:
            llist_lock.release()
            print('Thread {} completed parsing'.format(ident))
            break


def filebot(interval=60):
    global employees
    i = 0
    while True:
        time.sleep(interval)
        llist_lock.acquire()
        try:
            check = len(init_proto_profile_list)
        finally:
            llist_lock.release()

        elist_lock.acquire()
        try:
            chunk = employees
            employees = []
        finally:
            elist_lock.release()
        to_save = filter(None, chunk)
        data_frame = pandas.DataFrame.from_records(to_save)
        data_frame.to_csv('Century21_{}.csv'.format(i))
        i += 1
        print('\nFilebot autosaved to file at {}\n'.format(str(datetime.datetime.now())))

        if check < 3:
            break


def main():
    os.chdir('/mnt/c/Users/Liberty SBF/PycharmProjects/LSBF_Scrapers/Primitve_Scrapers/C21/')
    # os.chdir('C:\\Users\\Liberty SBF\\PycharmProjects\\LSBF_Scrapers\\Primitve_Scrapers\\C21')

    global employees
    global init_proto_profile_list
    for state in StateCodeList:
        init_proto_profile_list.append(
            'https://commercial.century21.com/search.c21?lid=S{}&t=2&o=&s=0&subView=searchView.Paginate'.format(state))
    startlength = len(init_proto_profile_list)
    threads = []

    for i in range(THREADCOUNT):
        thread = threading.Thread(target=threadbot, args=(i+1, startlength, ))
        threads.append(thread)
        thread.start()

    savethread = threading.Thread(target=filebot, args=())
    savethread.start()

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
