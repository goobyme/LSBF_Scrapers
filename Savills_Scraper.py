import requests
import bs4
import pandas
import re
import threading
import os

SEARCHPAGE = "http://www.savills-studley.com/contact/people-results.aspx?name=&country=3198&office=&sector="
THREADCOUNT = 10
employees = []
link_list = []
elist_lock = threading.Lock()
llist_lock = threading.Lock()


def webdl(url):
    """Downloads web-page (using requests rather than urllib) """
    print('Downloading...%s' % url)
    r = requests.get(url)
    try:
        r.raise_for_status()
        return r
    except requests.HTTPError:
        try:
            print('Download failed for %s' % url)
            return None
        except BlockingIOError:
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
    # soup = bs4.BeautifulSoup(page.text, 'lxml')
    # link_el = soup.find_all('a', {'class': 'dwn-vcard'})[0]
    to_dl = webdl('http://www.savills-studley.com/' + link)

    vcf_file = open('Savills_vcf_%s_%s.vcf' % (thread_ident, file_ident), 'wb')
    for chunk in to_dl.iter_content(100000):
        vcf_file.write(chunk)
    vcf_file.close()
    print('Wrote file %s-%s to file' % (thread_ident, file_ident))


def vcfparsing(file):
    # TODO make this cuz merging vcf manually SUCKS
    pass


def personparsing(page, link):
    """Parses text data from elements (not entire page) and outputs list of dictionaries with data"""
    es = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_el = soup.find('div', {'id', 'left_navigation_container'})
    e = {}

    name_el = parent_el.find('h1')
    e['Name'] = name_el.get_text()

    linkedin_el = parent_el.find('div', {'class': 'vx_block studley_linkedin'})
    if linkedin_el:
        li_link = linkedin_el.find('a')['href']
        e['LinkedIn'] = li_link

    specs =[]
    spec_el = parent_el.find_all('div', {'class': 'vx_block research_container_menu one_half'})
    if spec_el:
        for element in spec_el:
            text = element.find('a').get_text()
            specs += text + ', '
        e['Specialities'] = specs

    e['Profile Link'] = link

    es.append(e)
    return es


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses adding output to sublist or downloading vcf"""
    print('Thread %s Initialized' % ident)
    sublist = []
    vcf_links = []
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
            sp_parse = searchpageparsing(webdl(link))
            vcf_links += sp_parse.vlc_links
            for link in sp_parse.scrape:
                personparsing(webdl(link), link)
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

    print('Thread %s downloading vcf...' % ident)
    i = 1
    for link in vcf_links:
        vcfmuncher(link, ident, i)
        i += 1


def main():
    # os.chdir('/mnt/c/Users/Liberty SBF/Desktop/Savills_VCF')
    global employees
    global link_list
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
