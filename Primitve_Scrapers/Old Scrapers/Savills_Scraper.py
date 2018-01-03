import requests
import bs4
import pandas
import re
import threading
import os
import vobject

SEARCHPAGE = "http://www.savills-studley.com/contact/people-results.aspx?name=&country=3198&office=&sector="
THREADCOUNT = 10
employees = []
init_proto_profile_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()


def webdl(url):
    """Downloads web-page (using requests rather than urllib) """
    print('Downloading...%s' % url)
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r
    except requests.HTTPError:
        try:
            print('Download failed for %s' % url)
            return None
        except BlockingIOError:
            return None
    except requests.exceptions.MissingSchema:
        print('Download failed for %s' % url)
        return None


class ParsingOutput:
    """Used for outputting both scrapelist elements and links for VFC from searchpageparsing"""
    def __init__(self, scrape, vlc_links):

        self.scrape = scrape
        self.vlc_links = vlc_links


def searchpageparsing(page):
    """Scrapes search page for VCF links or information elements """

    contact_links = []
    vcf_links = []

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    contact_parent = soup.find_all('strong', {'class': 'highlight'})
    for element in contact_parent:
        contact_link = element.find('a', href=True)
        if contact_link:
            contact_links.append(contact_link['href'])

    vcf_parent = soup.find_all('span', {'class': 'action_link_business_card'})
    for element in vcf_parent:
        vcf_link = element.find('a', href=True)
        if vcf_link:
            vcf_links.append(vcf_link['href'].strip('.'))

    parse_result = ParsingOutput(contact_links, vcf_links)

    return parse_result


def vcfmuncher(link, thread_ident, file_ident):
    """Scrapes employee page for vcf and downloads it"""

    to_dl = webdl('http://www.savills-studley.com/' + link)
    file_name = 'Savills_vcf_%s_%s.vcf' % (thread_ident, file_ident)

    vcf_file = open(file_name, 'wb')
    for chunk in to_dl.iter_content(100000):
        vcf_file.write(chunk)
    vcf_file.close()
    print('Parsing VCF {}...'.format(file_name))
    try:
        parse_file = open(file_name, 'r').read()
    except UnicodeDecodeError:
        print('File reading error')
        return {}

    return vcfparsing(parse_file)


def vcfparsing(text):
    """Take vcard text contents, return JSON-structured employee."""
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
        e['City'] = vc.adr.value.city[0]
    except AttributeError or IndexError:
        pass
    try:
        e['PostalCode'] = vc.adr.value.code
    except AttributeError or IndexError:
        pass
    try:
        e['State'] = vc.adr.value.city[1]
    except:
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


def personparsing(page, link, vcf_link):
    """Parses text data from elements (not entire page) and outputs list of dictionaries with data"""
    es = []
    try:
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return None
    parent_el = soup.find('div', {'id': 'left_navigation_container'})
    e = {}

    if parent_el:
        name_el = parent_el.find('h1')
        e['Name'] = name_el.get_text()

        linkedin_el = parent_el.find('div', {'class': 'vx_block studley_linkedin'})
        if linkedin_el:
            li_link = linkedin_el.find('a')
            if li_link:
                e['LinkedIn'] = li_link['href']

        specs =[]
        spec_el = parent_el.find_all('div', {'class': 'vx_block research_container_menu one_half'})
        if spec_el:
            for element in spec_el:
                try:
                    text = element.find('a').get_text()
                    specs.append(text + ', ')
                except AttributeError:
                    continue
            e['Specialities'] = specs

    e['Profile Link'] = link
    e['VCF Link'] = vcf_link

    return e


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses adding output to sublist or downloading vcf"""
    print('Thread %s Initialized' % ident)
    sublist = []
    exportlist = []
    while True:
        llist_lock.acquire()
        if len(init_proto_profile_list) > 0:
            try:
                link = init_proto_profile_list[0]
                init_proto_profile_list.remove(link)
                length = len(init_proto_profile_list)
            finally:
                llist_lock.release()
            print('Thread %s parsing link %s of %s' % (ident, total_len - length, total_len))
            sp_parse = searchpageparsing(webdl(link))
            for n in range(len(sp_parse.scrape)):
                link = sp_parse.scrape[n]
                # Associate each vlc link to a name arbitrarily (will adjust if not every employee has a vcard)
                sublist.append(personparsing(webdl(link), link, sp_parse.vlc_links[n]))
        else:
            llist_lock.release()
            print('Thread %s completed parsing' % ident)
            break

    print('Thread %s downloading vcf...' % ident)
    sublist = filter(None, sublist)
    i = 1
    for profile in sublist:
        scraped_dic = vcfmuncher(profile['VCF Link'], ident, i)
        profile.update(scraped_dic)
        exportlist.append(profile)
        i += 1

    print('Thread %s writing to list...' % ident)
    elist_lock.acquire()
    try:
        global employees
        employees += exportlist
    finally:
        elist_lock.release()


def main():
    os.chdir('/mnt/c/Users/Liberty SBF/Desktop/Savills_VCF')
    global employees
    global init_proto_profile_list
    for i in range(55):
        link_list.append(SEARCHPAGE + '&page=%s' % str(i+1))
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
    data_frame.to_csv('SavillsStudley.csv')
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Add Time checking capabilities
# TODO Improve error handling
