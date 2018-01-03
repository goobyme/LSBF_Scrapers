import requests
import bs4
import pandas
import re
import threading
import os
import vobject
import codecs
import logging
from functools import wraps
import datetime
import time

THREADCOUNT = 20
employees = []
init_proto_profile_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()
err_lock = threading.Lock()


def webdl(url, retries=3):
    """Downloads web-page, retries on initial failures, returns None if fails thrice (common!)"""
    print('Downloading...{}'.format(url))
    for i in range(retries):
        try:
            r = requests.get(url)
            r.raise_for_status()
            return r
        except Exception:     # Ugly but it works, hard to account for all the possible error codes otherwise
            print('[Warning webdl]: Retrying Download')
            continue
    error_writer('webdl', 'Download failed for {}'.format(url))
    return None


def error_writer(func, message, thread='N/A'):
    fullmessage = '[Error-{} {}]: {}  {}'.format(thread, func, message, datetime.datetime.now())
    print(fullmessage)
    err_lock.acquire()
    try:
        error_dump = open('errordump.txt', 'a')
        error_dump.write(fullmessage)
        error_dump.close()
    finally:
        err_lock.release()


def inputvalidation(f):
    @wraps(f)
    def checkfornone(*args, **kwargs):
        for arg in args:
            if not arg:
                return None
            else:
                return f(*arg, **kwargs)
    return checkfornone


def employeelistparsing(page, thread_ident):
    """Parses each indv. profile listing page to return proto-profiles to be filled by visiting profile pages """
    if not page:
        return None

    profiles = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_el = soup.find('table')
    prof_els = parent_el.find_all('tr')
    try:
        assert prof_els
    except AssertionError:
        logging.exception('message')
        return None
    for el in prof_els:
        prof = personparsing(el, thread_ident)
        if prof:
            profiles.append(prof)

    return profiles


def personparsing(el, thread_ident):
    """Parses from some combination of vcf and page and outputs json (to be iterated outside of func)"""
    e = {}
    td_els = el.find_all('td')
    t = 1
    for td in td_els:
        if t == 1:
            name_el = el.find('h4')
            if name_el:
                name_raw = name_el.get_text()
                e['FULLNAME'] = name_raw.replace('\xa0', ' ')
            sub_els = td.find_all('span')
            t += 1
            n = 1
            for sub in sub_els:
                if n == 1:
                    try:
                        e['TITLE'] = sub.get_text()
                    except AttributeError or NameError:
                        continue
                    finally:
                        n += 1
                elif n == 2:
                    try:
                        e['COMPANY'] = sub.get_text()
                    except AttributeError or NameError:
                        continue
        elif t == 2:
            raw_text = td.get_text()
            phonecount = len(raw_text) / 12
            t += 1
            for i in range(int(phonecount)):
                formatnumber = raw_text[i * 12:(i * 12) + 12]
                e['BUSINESSPHONE_{}'.format(i+1)] = formatnumber
        elif t == 3:
            raw_text = td.get_text()
            phonecount = len(raw_text) / 12
            t += 1
            for i in range(int(phonecount)):
                formatnumber = raw_text[i * 12:(i * 12) + 12]
                e['MOBILEPHONE_{}'.format(i + 1)] = formatnumber
        elif t == 4:
            email_regex = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.com")
            raw_text = td.get_text()
            match = email_regex.findall(raw_text)
            if match:
                e['E-MAILADDRESS'] = match[0]
                new = raw_text.replace(match[0], '')
                e['PERSONALWEBSITE'] = new.strip()
        else:
            continue
    return e


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses to generate profile sublists , then merges with master"""
    print('Threadbot {} Initialized'.format(ident))

    """Iterated function over global list with hand-coded queueing"""
    # TODO integrate built in Queue functionality into threadbot
    def writetoglobal():
        global employees
        print('Thread {} writing to list...'.format(ident))
        elist_lock.acquire()
        try:
            employees += sublist
        finally:
            elist_lock.release()

    while True:
        sublist = []
        a = True
        for i in range(5):
            llist_lock.acquire()
            if len(init_proto_profile_list) > 0:
                try:
                    link = init_proto_profile_list[0]    # Take proto-profile from list and removes from queue
                    init_proto_profile_list.remove(link)
                    length = len(init_proto_profile_list)
                finally:
                    llist_lock.release()
                print('Thread {} parsing link {} of {}'.format(ident, total_len - length, total_len))
                # From here either feed through employeepageparsing or directly through personparsing
                p = employeelistparsing(webdl(link), ident)
                if p:
                    sublist += p
                else:
                    error_writer('employeelistparsing', 'Failed to parse {}'.format_map(link), str(ident))
                    continue
            else:
                llist_lock.release()
                print('Thread {} completed parsing'.format(ident))
                a = False
                break
        if a:
            writetoglobal()
        else:
            break

    """Final merge with global list"""
    writetoglobal()


def filebot(interval=60):
    global employees
    i = 0
    while True:
        time.sleep(interval)
        llist_lock.acquire()
        try:
            n = len(init_proto_profile_list)
        finally:
            llist_lock.release()
        if n > THREADCOUNT:
            elist_lock.acquire()
            try:
                chunk = employees
            finally:
                elist_lock.release()
            to_save = filter(None, chunk)
            data_frame = pandas.DataFrame.from_records(to_save)
            data_frame.to_csv('EasternUnion_{}.csv'.format(i))
            i += 1
            print('\nFilebot autosaved to file at {}\n'.format(str(datetime.datetime.now())))
        else:
            break


def main():
    os.chdir('C:\\Users\\James\\Desktop\\EasternUnion')
    global employees
    global init_proto_profile_list
    for i in range(3351, 5164):
        init_proto_profile_list.append('https://directory.easternuc.com/publicDirectory?page=' + str(i))

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
            data_frame.to_csv('EasternUnion.csv')
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
