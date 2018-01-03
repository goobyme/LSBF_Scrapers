import requests
import bs4
import pandas
import re
import threading
import os
import logging
import datetime
from functools import wraps

import vobject

"""Globals and Setup"""
THREADCOUNT = 20
SEARCHPAGE = 'INSERT URL HERE IF STARTPAGE IS NEEDED'

employees = []
init_proto_profile_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()
err_lock = threading.Lock()

os.chdir('INSERT WORKING DIRECTORY HERE')
if not os.path.isfile('errordump.txt'):
    f = open('errordump.txt', 'w')
    f.close()


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
    fullmessage = '\n[Error-{} {}]: {}  {}\n'.format(thread, func, message, datetime.datetime.now())
    print(fullmessage)
    err_lock.acquire()
    try:
        error_dump = open('errordump.txt', 'a')
        error_dump.write(fullmessage.strip())
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


@inputvalidation
def searchpageparsing(page):
    """Scrapes search page for individual parsing links to feed into threadbot system (not needed if pages # in url)"""
    employeepage_links = []

    soup = bs4.BeautifulSoup(page.text, 'lxml')

    # parent_element = soup.find('li', {'id': 'agent_offices_nav'})
    # link_elements = parent_element.find_all('li')
    # for el in link_elements:
    #     link_el = el.find('a')
    #     link = link_el['href']
    #
    #     new_page = webdl('https://www.elliman.com' + link)
    #     new_soup = bs4.BeautifulSoup(new_page.text, 'lxml')
    #
    #     sublink_popop = new_soup.find_all('div', {'class': 'w_aside_body'})
    #     for i in range(3,5):
    #         try:
    #             popop_el = sublink_popop[i]
    #         except IndexError:
    #             break
    #         sub_parent = popop_el.find_all('a', href=True)
    #         for sub_el in sub_parent:
    #             link = sub_el['href']
    #             employeepage_links.append(link)

    return employeepage_links


@inputvalidation
def employeelistparsing(page, thread_ident):
    """Parses each indv. profile listing page to return proto-profiles with info from search-page"""
    proto_profiles = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')

    prof_els = soup.find_all('tr')
    for el in prof_els:
        e = {}
        parent_el = el.find('td', {'class': 'first'})
        try:
            link_el = parent_el.find('a')
            e['FullName'] = link_el.text
            e['Link'] = link_el['href']
            proto_profiles.append(e)
        except AttributeError or TypeError:
            error_writer('employeelistparsing','Unable to find elements to form Proto-Profile')
            continue

    return proto_profiles


def vcfmuncher(link, thread_ident, name):
    """Downloads, saves, and edits (important!) VCF then feeds through parser to return employee profile json"""
    to_dl = webdl(link)

    if not to_dl:   # Handle webdl failure
        return None
    file_name = 'Elliman_{}_{}.vcf'.format(thread_ident, name)

    vcf_file = open(file_name, 'wb')
    for chunk in to_dl.iter_content(100000):
        vcf_file.write(chunk)
    vcf_file.close()
    print('Parsing VCF {}...'.format(file_name))
    try:    # Handle Unicode/read error
        parse_file = codecs.open(file_name, 'r', encoding='utf-8', errors='ignore')
        text_file = parse_file.read()
        parse_file.close()
    except UnicodeDecodeError:
        print('[Error-{} VCF Muncher]: File reading error'.format(thread_ident))
        return None

    """VCard Reformatter: Snips Vcard text so that Vobject can parse it without errors"""
    # photo_regex = re.compile(r"X-MS-CARDPICTURE.+(?=REV:)", flags=re.DOTALL)
    # address_regex = re.compile(r"(?<=0A=)\s+(?=[A-Z]|[0-9])")
    # line_regex = re.compile(r"-{10,}")
    # label_regex = re.compile(r"LABEL.+")    # ANNOYING! Prevent Unicode error (removes instances of quoted printable)
    #
    # photo_remove = photo_regex.sub('\n', text_file)
    # address_remove = address_regex.sub('', photo_remove, 3)
    # line_remove = line_regex.sub('', address_remove)
    # label_remove = label_regex.sub('', line_remove)
    e = vcfparsing(text_file, thread_ident)
    return e


def vcfparsing(text, thread_ident):
    """Take vcard text contents, return JSON-structured employee. Try funcitons really are necessary :(
       Change specific attributes to match vcard formatting. To check attribute of vc object use debugger"""

    e = {}
    """Unicode or VObject Parsing handling"""
    try:
        vc = vobject.readOne(text)
    except UnicodeDecodeError:
        print('[Warning-{} VCF Parsing]: Default encoding error'.format(thread_ident))
        return None
    except vobject.base.ParseError:
        logging.exception('message')
        return None

    """Parsing portion"""
    e['Prefix'] = vc.n.value.prefix
    e['FirstName'] = vc.n.value.given
    e['LastName'] = vc.n.value.family
    e['MiddleName'] = vc.n.value.additional
    e['Suffix'] = vc.n.value.suffix
    try:
        if type(vc.adr.value.street) is str:
            adr_strip = vc.adr.value.street.replace('\r', '')
            e['Street'] = adr_strip.replace('\n', ', ')
        elif type(vc.adr.value.street) is list:
            e['Street'] = ', '.join([x.strip()
                                     for x in vc.adr.value.street])
    except AttributeError:
        pass
    try:
        e['City'] = vc.adr.value.city
    except AttributeError:
        pass
    except IndexError:
        pass
    try:
        e['State'] = vc.adr.value.region
    except AttributeError:
        pass
    except IndexError:
        pass
    try:
        e['PostalCode'] = vc.adr.value.code
    except AttributeError:
        pass
    except IndexError:
        pass
    try:
        e['Email'] = vc.email.value
    except AttributeError:
        pass
    try:
        e['Company'] = vc.org.value[0]
    except AttributeError or IndexError:
        pass
    try:
        e['Title'] = vc.title.value
    except AttributeError:
        pass
    try:
        tel_list = vc.tel_list
        for tel in tel_list:
            number = tel.value
            categories = ''
            for cat in tel.singletonparams:
                categories += cat
            e[categories] = number

    except AttributeError:
        pass

    return e


@inputvalidation
def propertyhunter(page, type):
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    if type == 'C':
        prop_el = soup.find_all('td', {'class': 'last'})
    else:
        prop_el = soup.find_all('td', {'data-title': 'Type'})
    types = []
    for el in prop_el:
        if el.text in types:
            continue
        else:
            types.append(el.text)

    output = ""
    for type in types:
        output += type.strip() + ', '

    return output


def personparsing(page, thread_ident, profile, etype):
    """Parses from some combination of vcf and page and outputs json (to be iterated outside of func)"""
    try:    # Handle empty webdl failure
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return profile
    e = profile

    while etype == "B":
        link = ''
        parent_el = soup.find('div', {'class': 'wysiwyg team-members'})
        if not parent_el:
            break
        elif not parent_el.get_text():
            break
        potentials = parent_el.find_all('strong')
        for potential in potentials:
            link_el = potential.find('a')
            pot_name = link_el.text.strip
            if pot_name == profile['FullName']:
                link = link_el['href']
                break
        page = webdl(profile['Link'] + link)
        try:  # Handle empty webdl failure
            soup = bs4.BeautifulSoup(page.text, 'lxml')
            break
        except AttributeError:
            return profile

    """VCF parsing subsection, kills early if vcf parse fails"""
    if etype == 'C':
        vcf_el = soup.find('a', {'title': 'download vCard »'}, href=True)
        base_url = 'https://www.elliman.com'
    else:
        if etype == "A":
            base_url = os.path.dirname(os.path.dirname(e["Link"]))
        else:
            base_url = e["Link"]
        vcf_parent = soup.find('div', {'class': 'wysiwyg office-mobile _bigger'})
        vcf_el = vcf_parent.find('a', attrs={'title': False, 'href': True})
        if not vcf_el:
            vcf_parent = soup.find('div', {'class': 'wysiwyg _bigger'})
            vcf_el = vcf_parent.find('a', attrs={'title': False, 'href': True})
    if vcf_el:
        e['VCard'] = base_url + vcf_el['href']
    try:
        vcf_link = e['VCard']
        to_add = vcfmuncher(vcf_link, thread_ident, e['FullName'])
        if not to_add:
            print('[Error-{} personparsing FROM vcfmuncher]: VCF could not be downloaded/parsed'.format(thread_ident))
            return profile
        else:
            e.update(to_add)
    except KeyError:
        print('[Error-{} personparser]: VCF element could not be located'.format(thread_ident))
        return profile

    """Page parsing subsection, expand/comment out as needed"""
    if etype == "A":
        try:
            closed_parent = soup.find('div', {'class': 'w_tabs margin_vertical'})
            closed = closed_parent.find('li', {'class': 'last'})
            link_el = closed.find('a')
            e['Specialties'] = propertyhunter(webdl(e['Link'] + link_el['href']), etype)
        except AttributeError or TypeError:
            pass
    elif etype == 'C':
        closed = soup.find('a', {'title': 'Closed Transactions'})
        spec_els = soup.find_all('li', {'class': 'listing_extras'})
        if closed:
            link = closed['href']
            e['Specialties'] = propertyhunter(webdl('https://www.elliman.com' + link), etype)
        elif spec_els:
            to_dic = ''
            list_check = []
            for el in spec_els:
                if el.text not in list_check:
                    to_dic += el.text + ', '
                    list_check.append(el.text)
            e['Specialties'] = to_dic
    else:
        page = webdl(e['Link'] + '/closed-transactions')
        e['Specialties'] = propertyhunter(page, etype)

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
            # From here either feed through employeepageparsing or directly through personparsing
            protoprofiles = employeelistparsing(webdl('https://www.elliman.com' + link), ident)
            if not protoprofiles:
                continue
            seplinkregex = re.compile(r"https:")
            owner_empregex = re.compile(r"/about")
            for proprof in protoprofiles:
                try:
                    link = proprof['Link']
                except KeyError:
                    print('[Error-{} threadbot FROM employeelistparsing]: No link key in protoprof')
                    sublist.append(proprof)
                    continue
                """Splits employees by profile link: A) Direct, but external, B) Indirect and External, C) Internal"""
                if seplinkregex.findall(link):
                    if owner_empregex.findall(link):
                        full_prof = personparsing(webdl(link), ident, proprof, "A")
                    else:
                        full_link = link + '/about'
                        full_prof = personparsing(webdl(full_link), ident, proprof, "B")
                else:
                    full_link = 'https://www.elliman.com' + link
                    full_prof = personparsing(webdl(full_link), ident, proprof, "C")

                sublist.append(full_prof)
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
    os.chdir('C:\\Users\\James\\Desktop\\Elliman_VCF')
    global employees
    global init_proto_profile_list
    init_proto_profile_list = searchpageparsing(webdl(SEARCHPAGE))

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
            data_frame.to_csv('EllimanResidential_1.csv')
            break
        except PermissionError:
            print('Please close csv file')
            input()
    print(threadscomplete.sort())
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Add Time checking capabilities
# TODO Create an Error Dump for self-warning
