import requests
import bs4
import pandas
import re
import threading
import os
import vobject

SEARCHPAGE = "http://www.naiglobal.com/about-nai"
THREADCOUNT = 25
employees = []
link_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()


def webdl(url):
    """Downloads web-page (using requests rather than urllib), returns None if failed (common!)"""
    print('Downloading...%s' % url)
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r
    except:
        print('Download failed for %s' % url)
        return None

    # except requests.exceptions.MissingSchema:
    #     print('Download failed for %s' % url)
    #     return None


class ParsingOutput:
    """Used for outputting both scrapelist elements and links for VFC from searchpageparsing"""
    def __init__(self, scrape, vlc_links):

        self.scrape = scrape
        self.vlc_links = vlc_links


def searchpageparsing(page):
    """Scrapes search page for individual parsing links to feed into threadbot system (not needed if pages # in url)"""
    if not page:    # Failed webdl handling
        return None
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


def employeelistparsing(page):
    """Parses each indv. profile listing page to return specific person list, use in lieu of SPP if long list"""
    if not page:    # Handling failed webdl
        return None
    profile_links = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    elements = soup.find_all('div', {'class': 'tencol agentNTEPcontainer last'})
    for element in elements:
        name_parent = element.find('h3')
        link = name_parent.find('a')
        profile_links.append(link['href'])

    return profile_links


def vcfmuncher(link, thread_ident, file_ident):
    """Scrapes employee page for vcf and downloads it then feeds through parser to return employee profile dic"""

    to_dl = webdl('http://www.naiglobal.com/' + link)
    if not to_dl:   # Handle webdl failure
        return None
    file_name = 'NAI_vcf_%s_%s.vcf' % (thread_ident, file_ident)

    vcf_file = open(file_name, 'wb')
    for chunk in to_dl.iter_content(100000):
        vcf_file.write(chunk)
    vcf_file.close()
    vcf_file = open(file_name, 'a')
    vcf_file.write('\nEND:VCARD')
    vcf_file.close()
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

    vc = vobject.readOne(text)
    e['Prefix'] = vc.n.value.prefix
    e['FirstName'] = vc.n.value.given
    e['LastName'] = vc.n.value.family
    e['MiddleName'] = vc.n.value.additional
    e['Suffix'] = vc.n.value.suffix
    if type(vc.adr.value.street) is str:
        e['Street'] = vc.adr.value.street.replace('\n', ', ')
    elif type(vc.adr.value.street) is list:
        e['Street'] = ', '.join([x.strip()
                                        for x in vc.adr.value.street])
    try:
        e['PostalCode'] = vc.adr.value.code
    except AttributeError or IndexError:
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
        e['Phone'] = vc.tel.value
    except:
        pass

    return e


def personparsing(page, thread_ident, file_ident):
    """Parses from some combination of vcf and page and outputs list of dictionaries (iterate outside of func)"""
    try:    # Handle empty webdl failure
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return None

    linkedinregex = re.compile(r'linkedin\.com')
    """VCF parsing subsection, kills early if vcf parse fails"""
    vcf_parent = soup.find('span', {'id': 'dnn_lblVCardLink'})
    vcf_el = vcf_parent.find('a')
    if vcf_el:  # Handle failed vcf (possible fail points: webdl or File read error)
        vcf_link = vcf_el['href']
        e = vcfmuncher(vcf_link, thread_ident, file_ident)
    else:
        print('VCF link could not be downloaded/parsed')
        return None

    """Page parsing subsection, expand/comment out as needed"""
    city_el = soup.find('span', {'id': 'dnn_lblCityRegion'})
    if city_el:
        e['City'] = city_el.text

    state_el = soup.find('span', {'id': 'dnn_lblStateProvince'})
    if state_el:
        e['State'] = state_el.text

    spec_el = soup.find('span', {'id': 'dnn_ctr911_agentprofile_lblSpecialtyList'})
    if spec_el:
        spec = spec_el.get_text()
        e['Specialities'] = spec

    linked_el = soup.find('div', {'class': 'center dsmContainerAprofile'})
    try:    # TODO clean this up b/c linkedin will fail too often for this to be efficient
        links = linked_el.find_all('a', href=True)
        for link in links:
            if linkedinregex.findall(link['href']):
                e['LinkedIn'] = link['href']
    except AttributeError:
        pass

    return e


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses to generate profile sublists , then merges with master"""
    print('Thread %s Initialized' % ident)
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
            print('Thread %s parsing link %s of %s' % (ident, total_len - length, total_len))
            employee_links = employeelistparsing(webdl(link))
            if not employee_links:  # Handling empty employee links page
                break
            for link in employee_links:
                profile = personparsing(webdl(link), ident, i)
                sublist.append(profile)  # May return None values (will be filtered in main)
                i += 1
        else:
            llist_lock.release()
            print('Thread %s completed parsing' % ident)
            break

    print('Thread %s writing to list...' % ident)
    elist_lock.acquire()
    try:
        global employees
        employees += sublist
    finally:
        elist_lock.release()


def main():
    os.chdir('/mnt/c/Users/Liberty SBF/Desktop/NAI_VCF')
    global employees
    global link_list
    link_list = searchpageparsing(webdl(SEARCHPAGE))
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
    data_frame.to_csv('NAI_Rescrape.csv')
    print('Done')


if __name__ == "__main__":
    main()

# TODO PEP 8 cleanup (ie .format methods)
# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Add Time checking capabilities
# TODO Improve error handling
