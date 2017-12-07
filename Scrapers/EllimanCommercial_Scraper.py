import requests
import bs4
import pandas
import re
import threading
import os
import vobject
import codecs
import logging
import shelve

THREADCOUNT = 4
SEARCHPAGE = ['https://www.elliman.com/retail-and-commercial-brokerage/team',
              'https://thefaithconsoloteam.elliman.com/about',
              'https://thesrokaworldwideteam.elliman.com/about',
              'https://thedanacommercialteam.elliman.com/about']
employees = []
init_proto_profile_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()
team_lock = threading.Lock()


def webdl(url):
    """Downloads web-page, retries on initial failures, returns None if fails thrice (common!)"""
    print('Downloading...{}'.format(url))
    for i in range(3):
        try:
            r = requests.get(url)
            r.raise_for_status()
            return r
        except:     # Ugly but it works, hard to account for all the possible error codes otherwise
            print('[Warning webdl]: Retrying Download')
            continue
    print('[Error webdl]: Download failed for {}'.format(url))
    return None


def searchpageparsing(page):
    """Scrapes search page for individual parsing links to feed into threadbot system (not needed if pages # in url)"""
    if not page:    # Failed webdl handling
        return None
    proto_profiles = []

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_element = soup.find_all('dd', {'class': 'group'})

    for el in parent_element:
        e = {}
        link_el = el.find('a')
        if link_el:
            e['Link'] = link_el['href']
            e['Full Name'] = link_el.get_text()
        specialty_el = el.find('p', {'class': 'specialty'})
        if specialty_el:
            e['Specialty'] = specialty_el.get_text()
        proto_profiles.append(e)

    return proto_profiles


def employeelistparsing(page, link_type, base_name):
    """Parses each indv. profile listing page to return proto-profiles to be filled by visiting profile pages """
    if not page:    # Handling failed webdl
        return None

    proto_profiles = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')

    def whydifferingelnames(alphaomega, etype):
        elements = soup.find_all('div', {'class': '{}'.format(alphaomega)})
        for el in elements:
            e = {'Type': str(etype), 'BaseURL': base_name}
            parent_el = el.find('strong')
            link_el = parent_el.find('a', href=True)
            if link_el:
                e['Full Name'] = link_el.text.strip()
                e['Link'] = link_el['href']
            if etype == 'Main':
                subtag = el.find('div', {'class': '_grid8 _omega wysiwyg'})
                title_el = subtag.find('p')
                title = ''.join(title_el.find_all(text=True, recursive=False))
                if title_el:
                    e['Title'] = title.strip()
            proto_profiles.append(e)
    if link_type == 'Main':
        whydifferingelnames('_grid13 _alpha', 'Main')
        whydifferingelnames('_grid13 ', 'Main')
        whydifferingelnames('_grid13 _omega', 'Main')
    elif link_type == 'Affil':
        whydifferingelnames('grid_3 alpha', 'Affil')
        whydifferingelnames('grid_3 right-img', 'Affil')
        whydifferingelnames('grid_3 omega', 'Affil')

    return proto_profiles


def vcfmuncher(link, thread_ident, name, base_url):
    """Downloads, saves, and edits (important!) VCF then feeds through parser to return employee profile json"""
    to_dl = webdl(base_url + link)

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

    """VCard Reformatter: Snips Vcard text so that Vobject can parse it wihtout errors"""
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


def propertyhunter(page, type):
    if not page:    # Handling failed webdl
        return None
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    if type == 'Affil':
        prop_el = soup.find_all('td', {'data-title': 'Type'})
    else:
        prop_el = soup.find_all('td', {'class': 'last'})
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


def personparsing(page, thread_ident, profile):
    """Parses from some combination of vcf and page and outputs json (to be iterated outside of func)"""

    try:    # Handle empty webdl failure
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return profile
    e = profile

    """VCF parsing subsection, kills early if vcf parse fails"""
    if e['Type'] == 'Main':
        vcf_el = soup.find('a', {'title': 'download vCard »'}, href=True)
    else:
        vcf_parent = soup.find('div', {'class': 'wysiwyg office-mobile _bigger'})
        vcf_el = vcf_parent.find('a', attrs={'title': False, 'href': True})
    if vcf_el:
        e['VCard'] = vcf_el['href']
    try:
        vcf_link = e['VCard']
        to_add = vcfmuncher(vcf_link, thread_ident, e['Full Name'], e['BaseURL'])
        if not to_add:
            print('[Error-{} vcfmuncher]: VCF could not be downloaded/parsed'.format(thread_ident))
            return profile
        else:
            e.update(to_add)
    except KeyError:
        print('[Error-{} personparser]: VCF element could not be located'.format(thread_ident))
        return profile

    """Page parsing subsection, expand/comment out as needed"""
    if e['Type'] == 'Main':
        closed = soup.find('a', {'title': 'Closed Transactions'})
        spec_els = soup.find_all('li', {'class': 'listing_extras'})
        if closed:
            link = closed['href']
            e['Specialties'] = propertyhunter(webdl('https://www.elliman.com' + link), 'Main')
        elif spec_els:
            to_dic = ''
            list_check = []
            for el in spec_els:
                if el.text not in list_check:
                    to_dic += el.text + ', '
                    list_check.append(el.text)
            e['Specialties'] = to_dic
        else:
            pass
    else:
        closed = soup.find('li', {'class': 'last'})
        link_el = closed.find('a')
        if link_el.text == 'Closed Transactions':
            e['Specialties'] = propertyhunter(webdl(e['BaseURL'] + e['Link'] + link_el['href']), 'Affil')

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
                profile = init_proto_profile_list[0]    # Take proto-profile from list and removes from queue
                init_proto_profile_list.remove(profile)
                length = len(init_proto_profile_list)
            finally:
                llist_lock.release()
            print('Thread {} parsing link {} of {}'.format(ident, total_len - length, total_len))
            try:        # From here either feed through employeepageparsing or directly through personparsing
                link = profile['BaseURL'] + profile['Link']
            except KeyError:
                print('[Error-{} Threadbot]: No link in proto-prof'.format(ident))
                continue
            full_prof = personparsing(webdl(link), ident, profile)
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
    os.chdir('/mnt/c/Users/James/Desktop/Elliman_VCF')
    # os.chdir('C:\\Users\\James\\Desktop\\Elliman_VCF')
    global employees
    global init_proto_profile_list
    for i in range(len(SEARCHPAGE)):
        if i == 0:
            etype = 'Main'
            base_name = 'https://www.elliman.com'
        else:
            etype = "Affil"
            base_name = os.path.dirname(SEARCHPAGE[i])
        init_proto_profile_list += employeelistparsing(webdl(SEARCHPAGE[i]), etype, base_name)

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
            data_frame.to_csv('EllimanCommercial.csv')
            break
        except PermissionError:
            print('Please close csv file')
            input()
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Add Time checking capabilities
# TODO Improve error handling
