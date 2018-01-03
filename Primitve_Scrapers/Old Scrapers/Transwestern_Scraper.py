import requests
import bs4
import re
import threading
import os

SEARCHPAGE = 'https://www.transwestern.com/ourcompany/locations'
COMPANYFUNCTIONS = ['Agency%20Leasing', 'Tenant%20Advisory', 'Capital%20Markets', 'Asset%20Services', 'Sustainability']
THREADCOUNT = 10
init_proto_profile_list = []
llist_lock = threading.Lock()


def webdl(url):
    """Downloads web-page (using requests rather than urllib) """
    print('Downloading...%s' % url)
    r = requests.get(url)
    try:
        r.raise_for_status()
        return r
    except requests.HTTPError:
        print('Download failed for %s' % url)
        return None


class ParsingOutput:
    """Used for outputting both scrapelist elements and links for VFC from searchpageparsing"""
    def __init__(self, scrape, vlc_links):

        self.scrape = scrape
        self.vlc_links = vlc_links


def searchpageparsing(page, tag, idclass):
    """Scrapes search page for team links by location (can expand to search NAI globally)"""
    scrapelist = []

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_elements = soup.find_all(tag, idclass)

    for element in parent_elements:
        if tag != 'a':
            link_element = element.find_all('a')
            try:
                link = link_element[0]['href']
                scrapelist.append(link)
            except IndexError:
                continue
        else:
            link = element['href']
            scrapelist.append(link)

    return scrapelist


def employeeparsing(page):

    vcard_list = []

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_elements = soup.find_all('a', {'class': 'OpenSansBlue10'})
    

    for element in parent_elements:
        if 'type=vcard' in element['href']:
            vcard_list.append(element['href'])
        else:
            continue

    to_return = ParsingOutput( x , vcard_list)
    return vcard_list


def threadbot(ident, total_len):

    print('Thread %s Initialized' % ident)
    i = 1
    while True:
        llist_lock.acquire()
        if len(init_proto_profile_list) > 0:
            try:
                length = len(init_proto_profile_list)
                link = init_proto_profile_list[0]
                init_proto_profile_list.remove(link)
            finally:
                llist_lock.release()
            print('Thread %s downloading page %s of %s' % (ident, total_len-length, total_len))

            employeelinks = []
            for func in COMPANYFUNCTIONS:
                page = webdl('https://www.transwestern.com' + link + '/ourcompany/localcontacts?serviceline=' + func)
                employeelinks += searchpageparsing(page, 'div', {'class', 'person_name'})

            vcf_links = []
            for link in employeelinks:
                page = webdl('https://www.transwestern.com' + link)
                vcf_links += employeeparsing(page)

            for link in vcf_links:
                to_dl = webdl('https://www.transwestern.com' + link)
                vcf_file = open('transwestern_vcf_%s_%s.vcf' % (ident, i), 'wb')
                for chunk in to_dl.iter_content(100000):
                    vcf_file.write(chunk)
                vcf_file.close()
                print('Wrote file %s-%s to file' % (ident, i))
                i += 1
        else:
            llist_lock.release()
            print('Thread %s completed parsing' % ident)
            break


def main():

    os.chdir('/mnt/c/Users/Liberty SBF/Desktop/Transwestern_VCF')

    global init_proto_profile_list
    link_list = searchpageparsing(webdl(SEARCHPAGE), 'a', {'class': 'OpenSansBlue18'})
    startlength = len(link_list)
    threads = []

    for i in range(THREADCOUNT):
        thread = threading.Thread(target=threadbot, args=(i+1, startlength, ))

        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Improve progress checking capabilities
# TODO Improve error handling
