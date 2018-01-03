import requests
import bs4
import pandas
import re
import threading
import os
import vobject
import codecs
import logging
import chardet

THREADCOUNT = 15
SEARCHPAGE = 'http://www.kiddermathews.com/professionals_results.php?select_criteria=by_name&search=&type=search&search_submit=Search'
employees = []
init_proto_profile_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()


def webdl(url):
    """Downloads web-page (using requests rather than urllib), returns None if failed (common!)"""
    print('Downloading...{}'.format(url))
    for i in range(3):
        try:
            r = requests.get(url)
            r.raise_for_status()
            return r
        except:
            print('[Warning webdl]: Retrying Download')
            continue
    print('[Error webdl]: Download failed for {}'.format(url))
    return None


def searchpageparsing(page):    # Note for initial Coldwell this was run seperately, for more managable errors
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


def employeelistparsing(page):
    """Parses each indv. profile listing page to return specific person list, use in lieu of SPP if long list"""
    if not page:    # Handling failed webdl
        return None
    proto_profiles = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    elements = soup.find_all('tr')
    for element in elements:
        e = {}
        link_parent = element.find('h3')
        if link_parent:
            link_el = link_parent.find('a')
        try:
            e['Link'] = link_el['href']
            e['Full Name'] = link_el.text
        except TypeError:
            continue
        department_el = element.find('td', {'width': '35%'})
        if department_el:
            e['Department'] = department_el.text.strip()

        proto_profiles.append(e)

    return proto_profiles


def vcfmuncher(link, thread_ident, name):
    """Scrapes employee page for vcf and downloads it then feeds through parser to return employee profile dic"""

    to_dl = webdl('http://www.kiddermathews.com' + link)
    if not to_dl:   # Handle webdl failure
        return None
    file_name = 'Kidder_{}_{}.vcf'.format(thread_ident, name)

    vcf_file = open(file_name, 'wb')
    for chunk in to_dl.iter_content(100000):
        vcf_file.write(chunk)
    vcf_file.close()
    # vcf_file = open(file_name, 'a')   # Use this to write to vcf manually if vobject doesnt like it
    # vcf_file.write('\nEND:VCARD')
    # vcf_file.close()
    print('Parsing VCF {}...'.format(file_name))
    try:    # Handle Unicode/read error
        parse_file = codecs.open(file_name, 'r', encoding='utf-8', errors='ignore')
        text_file = parse_file.read()
        parse_file.close()
    except UnicodeDecodeError:
        print('[Error-{} VCF Muncher]: File reading error'.format(thread_ident))
        return None

    """VCard Reformatter: Snips Vcard text so that Vobject can parse it wihtout errors"""
    photo_regex = re.compile(r"X-MS-CARDPICTURE.+(?=REV:)", flags=re.DOTALL)
    address_regex = re.compile(r"(?<=0A=)\s+(?=[A-Z]|[0-9])")
    line_regex = re.compile(r"-{10,}")
    label_regex = re.compile(r"LABEL.+")    # ANNOYING! Prevent Unicode error (removes instances of quoted printable)
    photo_remove = photo_regex.sub('\n', text_file)
    address_remove = address_regex.sub('', photo_remove, 3)
    line_remove = line_regex.sub('', address_remove)
    label_remove = label_regex.sub('', line_remove)
    e = vcfparsing(label_remove, thread_ident)
    return e


def vcfparsing(text, thread_ident):
    """Take vcard text contents, return JSON-structured employee. Try funcitons really are necessary :("""
    # TODO replace with a hand-made regex b/c seriously vobject sucks and is poorly documented
    e = {}

    try:
        vc = vobject.readOne(text)
    except UnicodeDecodeError:
        print('[Warning-{} VCF Parsing]: Default encoding error'.format(thread_ident))
        try:
            vc = vobject.readOne(text)
        except UnicodeDecodeError:
            print('[Error-{} VCF Parsing]: Could not decode str'.format(thread_ident))
            logging.exception('message')
            return None
    except vobject.base.ParseError:
        logging.exception('message')
        return None
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


def personparsing(page, thread_ident, profile):
    """Parses from some combination of vcf and page and outputs list of dictionaries (iterate outside of func)"""
    try:    # Handle empty webdl failure
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return profile
    e = profile

    """VCF parsing subsection, kills early if vcf parse fails"""
    vcfregex = re.compile(r"\.vcf")
    vcf_parent = soup.find_all('a', {'class': 'link download'}, href=True)
    for potential_link in vcf_parent:
        pot_link = potential_link['href']
        if vcfregex.findall(pot_link):
            e['VCard'] = pot_link.replace('.', '', 2)
        else:
            e['Bio'] = pot_link.replace('.', '', 2)
    try:
        vcf_link = e['VCard']
        to_add = vcfmuncher(vcf_link, thread_ident, e['Full Name'])
        if not to_add:
            print('[Error-{} vcfmuncher]: VCF could not be downloaded/parsed'.format(thread_ident))
            return profile
        else:
            e.update(to_add)
    except KeyError:
        print('[Error-{} personparser]: VCF element could not be located'.format(thread_ident))
        return profile

    # """Page parsing subsection, expand/comment out as needed"""
    # def pythonicparser(title, bs4):
    #     spec_parent = soup.find(bs4)
    #     if spec_parent:
    #         spec_el = spec_parent.find_all('li')
    #         combined_spec = ''
    #         for el in spec_el:
    #             if el.get_text:
    #                 spec = el.get_text()
    #                 combined_spec += spec + ', '
    #         e[str(title)] = combined_spec
    #
    # pythonicparser('Specialities', "'div', {'id': MasterPage_ctl00_ContentPlaceHolder1_divAreasOfSpecialization")
    #
    # experience_parents = soup.find_all('span', {'style': 'font-size: 8pt; font-weight: bold;'})
    # for el in experience_parents:
    #     if el.get_text() == 'Years of Experience':
    #         outer_el = el.parent
    #         exp = outer_el.text.replace('Years of Experience', '')
    #         e['Experience'] = exp.strip()
    #     else:
    #         continue

    return e


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses to generate profile sublists , then merges with master"""
    print('Threadbot {} Initialized'.format(ident))
    sublist = []
    while True:
        llist_lock.acquire()
        if len(init_proto_profile_list) > 0:
            try:
                profile = init_proto_profile_list[0]
                init_proto_profile_list.remove(profile)
                length = len(init_proto_profile_list)
            finally:
                llist_lock.release()
            print('Thread {} parsing link {} of {}'.format(ident, total_len - length, total_len))
            try:
                link = profile['Link']
                full_link = 'http://www.kiddermathews.com/' + link
            except KeyError:
                continue
            full_prof = personparsing(webdl(full_link), ident, profile)
            sublist.append(full_prof)
        else:
            llist_lock.release()
            print('Thread {} completed parsing'.format(ident))
            break

    print('Thread {} writing to list...'.format(ident))
    elist_lock.acquire()
    try:
        global employees
        employees += sublist
    finally:
        elist_lock.release()


def main():
    os.chdir('/mnt/c/Users/Liberty SBF/Desktop/Kidder_VCF_Copy')
    # os.chdir('C:\\Users\\Liberty SBF\\Desktop\\Kidder_VCF')
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
            data_frame.to_csv('Kidder.csv')
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
