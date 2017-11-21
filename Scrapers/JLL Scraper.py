import requests
import bs4
import pandas
import re
import threading
import os
import vobject

SEARCHPAGE = "https://www.cbcworldwide.com/professionals/profile/A2E3B313-AD7E-435C-86B8-905EF0B23428#h=2mGrlX-1"
THREADCOUNT = 20
employees = []
link_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()


def webdl(url):
    """Downloads web-page (using requests rather than urllib), returns None if failed (common!)"""
    print('Downloading...{}'.format(url))
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r
    except:
        print('Download failed for {}'.format(url))
        return None

    # except requests.exceptions.MissingSchema:
    #     print('Download failed for %s' % url)
    #     return None


def searchpageparsing(page):    # Note for initial Coldwell this was run seperately, for more managable errors
    """Scrapes search page for individual parsing links to feed into threadbot system (not needed if pages # in url)"""
    if not page:    # Failed webdl handling
        return None
    scrapelist = []

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_element = soup.find('a', {'id': 'resultsNext'})

    while parent_element:
        link = parent_element['href']
        scrapelist.append(link)
        page = webdl('https://www.cbcworldwide.com' + link)

        soup = bs4.BeautifulSoup(page.text, 'lxml')
        parent_element = soup.find('a', {'id': 'resultsNext'})

    return scrapelist


def employeelistparsing(page):
    """Parses each indv. profile listing page to return specific person list, use in lieu of SPP if long list"""
    if not page:    # Handling failed webdl
        return None
    profile_links = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    elements = soup.find_all('a', {'class': 'card-photo'})
    for element in elements:
        try:
            link = element['href']
            profile_links.append(link)
        except IndexError:
            continue

    return profile_links


def vcfmuncher(link, thread_ident, file_ident):
    """Scrapes employee page for vcf and downloads it then feeds through parser to return employee profile dic"""

    to_dl = webdl('https://www.cbcworldwide.com' + link)
    if not to_dl:   # Handle webdl failure
        return None
    file_name = 'Coldwell_vcf_%s_%s.vcf' % (thread_ident, file_ident)

    vcf_file = open(file_name, 'wb')
    for chunk in to_dl.iter_content(100000):
        vcf_file.write(chunk)
    vcf_file.close()
    # vcf_file = open(file_name, 'a')   # Use this to write to vcf manually if vobject doesnt like it
    # vcf_file.write('\nEND:VCARD')
    # vcf_file.close()
    print('Parsing VCF {}...'.format(file_name))
    try:    # Handle Unicode/read error
        parse_file = open(file_name, 'r')
        text_file = parse_file.read()
        parse_file.close()
    except UnicodeDecodeError:
        print('File reading error')
        return None

    e = vcfparsing(text_file)
    return e


def vcfparsing(text):
    """Take vcard text contents, return JSON-structured employee. Try funcitons really are necessary :("""
    # TODO replace with a hand-made regex b/c seriously vobject sucks and is poorly documented
    e = {}
    workphoneregex = re.compile(r'(?<=TEL;WORK;VOICE:)\d+(?=\n)')
    workfaxregex = re.compile(r'(?<=TEL;WORK;FAX:)\d+(?=\n)')
    cellregex = re.compile(r'(?<=TEL;CELL;VOICE:)\d+(?=\n)')

    vc = vobject.readOne(text)
    e['Prefix'] = vc.n.value.prefix
    e['FirstName'] = vc.n.value.given
    e['LastName'] = vc.n.value.family
    e['MiddleName'] = vc.n.value.additional
    e['Suffix'] = vc.n.value.suffix
    try:
        if type(vc.adr.value.street) is str:
            e['Street'] = vc.adr.value.street.replace('\n', ', ')
        elif type(vc.adr.value.street) is list:
            e['Street'] = ', '.join([x.strip()
                                     for x in vc.adr.value.street])
    except AttributeError:
        pass
    try:
        e['City'] = vc.adr.value.city[0]
    except AttributeError:
        pass
    except IndexError:
        pass
    try:
        e['State'] = vc.adr.value.city[1]
    except AttributeError:
        pass
    except IndexError:
        pass
    try:
        e['PostalCode'] = vc.adr.value.city[2]
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

    phone = workphoneregex.findall(text)
    if phone:
        e['Work Phone'] = phone[0]
    fax = workfaxregex.findall(text)
    if fax:
        e['Fax'] = fax[0]
    cell = cellregex.findall(text)
    if cell:
        e['Mobile'] = cell[0]

    return e


def personparsing(page, thread_ident, file_ident, link):
    """Parses from some combination of vcf and page and outputs list of dictionaries (iterate outside of func)"""
    try:    # Handle empty webdl failure
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return None
    e = {}
    """VCF parsing subsection, kills early if vcf parse fails"""
    # vcf_parent = soup.find('a', {'data-ga-click-action': 'download-professional-v-card'})
    # vcf_el = vcf_parent['href']
    # if vcf_el:  # Handle failed vcf (possible fail points: webdl or File read error)
    #     e = vcfmuncher(vcf_el, thread_ident, file_ident)
    #     if not e:
    #         print('VCF could not be downloaded/parsed')
    #         return None
    # else:
    #     print('VCF could not be found')
    #     return None

    """Page parsing subsection, expand/comment out as needed"""
    parent_el = soup.find('div', {'class': 'sidebar profile'})
    name_el = parent_el.find('h2')
    if name_el:
        e['Name'] = name_el.text
    title_el = parent_el.find('h3')
    if title_el:
        e['Title'] = title_el.text
    phone_el = parent_el.find('p', {'class': 'phone'})
    if phone_el:
            e['Phone'] = phone_el.text
    email_el = parent_el.find('p', {'class': 'email'})
    if email_el:
        e['Email'] = email_el.get_text()

    address_parent = parent_el.find('p', {'class': 'bio-address'})
    if address_parent:
        address_1 = address_parent.find('span', {'class': 'bio-address-line1'})
        if address_1:
            e['Address Line 1'] = address_1.text
        address_2 = address_parent.find('span', {'class': 'bio-address-line2'})
        if address_2:
            e['Address Line 2'] = address_2.text
        city_el = address_parent.find('span', {'class': 'bio-address-city'})
        if city_el:
            e['City'] = city_el.text
        state_el = address_parent.find('span', {'class': 'bio-address-state'})
        if state_el:
            e['State'] = state_el.text
        postal_el = address_parent.find('span', {'class': 'bio-address-postalcode'})
        if postal_el:
            e['PostalCode'] = postal_el.text

    e['Profile Link'] = link

    return e


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses to generate profile sublists , then merges with master"""
    print('Threadbot {} Initialized'.format(ident))
    sublist = []
    i =1    # For VCF file naming
    while True:
        llist_lock.acquire()
        if len(link_list) > 0:
            try:
                link = link_list[0]
                link_list.remove(link)
                length = len(link_list)
            finally:
                llist_lock.release()
            print('Thread {} parsing link {} of {}'.format(ident, total_len - length, total_len))
            # TODO enter in profile page links via list created by JLL Helper

            link = 'http://www.us.jll.com' + link
            profile = personparsing(webdl(link), ident, i, link)
            sublist.append(profile)  # May return None values (will be filtered in main)
            i += 1
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
    os.chdir('/mnt/c/Users/Liberty SBF/Desktop/Coldwell_VCF')
    global employees
    global link_list
    for i in range(181):
        link = 'https://www.cbcworldwide.com/professionals/find/page?page={}'.format(i+1)
        link_list.append(link)
    if not link_list:   # Handle early error in searchpage
        print('Search page parsing failed. No link list generated. Closing scraper')
        return None
    startlength = len(link_list)
    threads = []

    for i in range(THREADCOUNT):
        thread = threading.Thread(target=threadbot, args=(i+1, startlength, ))

        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    to_save = filter(None, employees)   # Gets rid of NoneTypes in list (very annoying if you don't do this!)
    data_frame = pandas.DataFrame.from_records(to_save)
    data_frame.to_csv('JLL.csv')
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Add Time checking capabilities
# TODO Improve error handling
