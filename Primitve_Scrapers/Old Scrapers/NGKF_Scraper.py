import requests
import bs4
import pandas
import re
import threading
import os
import vobject
import shelve

THREADCOUNT = 20
employees = []
init_proto_profile_list = ['/home/about-our-firm/global-offices/us-offices/new-york/professional-profiles.aspx',
 '/home/about-our-firm/global-offices/us-offices/phoenix/professional-profiles.aspx',
 '/home/about-our-firm/global-offices/us-offices/bentonville/professional-profiles.aspx',
 '/home/about-our-firm/global-offices/us-offices/little-rock/professional-profiles.aspx',
 '/home/about-our-firm/global-offices/us-offices/bakersfield/professional-profiles.aspx',
 '/home/about-our-firm/global-offices/us-offices/emeryville/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/fresno/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/hayward/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/los-angeles/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/newport-beach/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/ontario/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/palo-alto/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/pleasanton/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/roseville/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/sacramento/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/san-diego/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/san-francisco/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/san-mateo/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/san-rafael/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/santa-clara/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/santa-rosa/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/visalia/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/walnut-creek/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/colorado-springs/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/denver/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/connecticut/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/connecticut/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/wilmington-de/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/washington-dc/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/boca-raton/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/jacksonville/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/miami/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/orlando/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/sarasota/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/tampa/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/atlanta/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/honolulu/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/chicago/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/fort-wayne/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/indianapolis/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/mishawaka/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/baltimore/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/boston/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/detroit/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/grand-rapids/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/minneapolis/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/kansas-city/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/st-louis/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/las-vegas/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/new-jersey---central/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/new-jersey---north/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/new-jersey---south/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/new-york/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/new-york---brooklyn/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/long-island/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/westchester/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/charlotte/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/fargo/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/cincinnati/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/cleveland/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/columbus/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/oklahoma-city/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/tulsa/nkf-professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/portland/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/philadelphia/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/pittsburgh/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/wayne-pa/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/charleston/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/columbia/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/greenville/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/myrtle-beach/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/memphis/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/austin/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/dallas/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/houston/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/american-fork/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/clearfield/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/salt-lake-city/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/tysons-/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/bellevue/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/seattle/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/appleton/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/green-bay/professional-profiles.aspx',
                           '/home/about-our-firm/global-offices/us-offices/wausau/professional-profiles.aspx']
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
        print('[Error webdl]: Download failed for {}'.format(url))
        return None


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


def vcfmuncher(link, thread_ident, file_ident):
    """Scrapes employee page for vcf and downloads it then feeds through parser to return employee profile dic"""

    to_dl = webdl('http://www.ngkf.com' + link)
    if not to_dl:   # Handle webdl failure
        return None
    file_name = 'NGKF_vcf_{}_{}.vcf'.format(thread_ident, file_ident)

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
    workphoneregex = re.compile(r'(?<=WORK;VOICE:).+(?=\n)')
    workfaxregex = re.compile(r'(?<=TEL;WORK;FAX:).+(?=\n)')
    cellregex = re.compile(r'(?<=CELL;VOICE:).+(?=\n)')

    vc = vobject.readOne(text)
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


def personparsing(page, thread_ident, file_ident, proto_prof):
    """Parses from some combination of vcf and page and outputs list of dictionaries (iterate outside of func)"""
    try:    # Handle empty webdl failure
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return None
    e = proto_prof

    """VCF parsing subsection, kills early if vcf parse fails"""
    vcf_parent = soup.find('a', {'class': 'v-card'})
    if vcf_parent:
        vcf_el = vcf_parent['href']
        if vcf_el:  # Handle failed vcf (possible fail points: webdl or File read error)
            to_add = vcfmuncher(vcf_el, thread_ident, file_ident)
            if not to_add:
                print('[Error vcfmuncher]: VCF could not be downloaded/parsed')
                return e
            else:
                e.update(to_add)
        else:
            print('[Error personparser]: VCF link could not be found')
            return e
    else:
        print('[Error personparser]: VCF element could not be located')
        return e

    """Page parsing subsection, expand/comment out as needed"""
    def pythonicparser(title, bs4):
        spec_parent = soup.find(bs4)
        if spec_parent:
            spec_el = spec_parent.find_all('li')
            combined_spec = ''
            for el in spec_el:
                if el.get_text:
                    spec = el.get_text()
                    combined_spec += spec + ', '
            e[str(title)] = combined_spec

    pythonicparser('Specialities', "'div', {'id': MasterPage_ctl00_ContentPlaceHolder1_divAreasOfSpecialization")

    experience_parents = soup.find_all('span', {'style': 'font-size: 8pt; font-weight: bold;'})
    for el in experience_parents:
        if el.get_text() == 'Years of Experience':
            outer_el = el.parent
            exp = outer_el.text.replace('Years of Experience', '')
            e['Experience'] = exp.strip()
        else:
            continue

    return e


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses to generate profile sublists , then merges with master"""
    print('Threadbot {} Initialized'.format(ident))
    sublist = []
    i =1    # For VCF file naming
    while True:
        llist_lock.acquire()
        if len(init_proto_profile_list) > 0:
            try:
                link = init_proto_profile_list[0]
                init_proto_profile_list.remove(link)
                length = len(init_proto_profile_list)
            finally:
                llist_lock.release()
            print('Thread {} parsing link {} of {}'.format(ident, total_len - length, total_len))
            full_link = 'http://www.ngkf.com' + link
            proto_prof = employeelistparsing(webdl(full_link))
            if not proto_prof:  # Handling empty employee links page
                print('[Error employeepageparser]: Unable to parse employee page for {}'.format(link))
                continue
            for prof in proto_prof:
                link = 'http://www.ngkf.com' + prof['Link']
                profile = personparsing(webdl(link), ident, i, prof)
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
    os.chdir('/mnt/c/Users/Liberty SBF/Desktop/NGKF_VCF')
    # os.chdir('C:\\Users\\Liberty SBF\\Desktop\\NGKF_VCF')
    global employees
    global init_proto_profile_list
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
    data_frame.to_csv('NGKF_1.csv')
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Add Time checking capabilities
# TODO Improve error handling
