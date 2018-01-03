import requests
import bs4
import pandas
import re
import threading
import os

SEARCHPAGE = "http://www.cushmanwakefield.com/en/people/search-results/?q=&loc=%7bFD83F41C-811B-462C-B684-B15B0456D634%7d"
THREADCOUNT = 25
employees = []
init_proto_profile_list = []
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
    scrapelist = []
    vcf_list = []

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_elements = soup.find_all('li', {'class': 'resultItem'})

    for element in parent_elements:
        vcf_flag = element.find('a', {'class': 'button flatButton'})
        if vcf_flag:
            vcf_list.append('http://www.cushmanwakefield.com' + vcf_flag['href'])
        else:
            scrapelist.append(element)

    parse_result = ParsingOutput(scrapelist, vcf_list)

    return parse_result


def vcfmuncher(page, thread_ident, file_ident):
    """Scrapes employee page for vcf and downloads it"""
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    link_el = soup.find_all('a', {'class': 'dwn-vcard'})[0]
    to_dl = webdl(link_el['href'])

    vcf_file = open('cushmanwakefield_vcf_%s_%s.vcf' % (thread_ident, file_ident), 'wb')
    for chunk in to_dl.iter_content(100000):
        vcf_file.write(chunk)
    vcf_file.close()
    print('Wrote file %s-%s to file' % (thread_ident, file_ident))


def personparsing(elements):
    """Parses text data from elements (not entire page) and outputs list of dictionaries with data"""
    es = []
    phoneregex = re.compile(r"(?<=tel:).+")
    emregex = re.compile(r"(?<=mailto:).+")

    for element in elements:
        e = {}
        name = element.find_all('h3')
        try:
            e["Name"] = name[0].get_text().strip()
        except IndexError:
            pass

        titles = element.find_all('p', {'class': 'title'})
        for title in titles:
            if title.get_text():
                e['Title'] = title.get_text()
            else:
                continue

        try:
            email_parent = element.find_all('a', {'class': 'button'})[0]
            email = emregex.findall(email_parent['href'])
            e["Email"] = email[0]
        except IndexError:
            pass

        loc_parent = element.find_all('p', {'class': None})
        if loc_parent:
            loc_text = loc_parent[0].get_text()
            if loc_text:
                e["Location"] = loc_text

        phone_parent = element.find_all('a', {'class': 'phoneNumber'})
        if phone_parent:
            i = 1
            for phone_el in phone_parent:
                phone = phoneregex.findall((phone_el['href']))
                try:
                    e["Phone_%s" % i] = phone[0]
                    i += 1
                except IndexError:
                    pass
        es.append(e)
    return es


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses adding output to sublist or downloading vcf"""
    print('Thread %s Initialized' % ident)
    sublist = []
    vcf_links = []
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
            vcf_links += sp_parse.vlc_links
            sublist += personparsing(sp_parse.scrape)
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
        vcfmuncher(webdl(link), ident, i)
        i += 1


def main():

    os.chdir('/mnt/c/Users/Liberty SBF/Desktop/Cushman_VCF')
    global employees
    global init_proto_profile_list
    for i in range(547):
        link_list.append(SEARCHPAGE + '&page=%s' % str(i+1))
    startlength = len(link_list)
    threads = []

    for i in range(THREADCOUNT):
        thread = threading.Thread(target=threadbot, args=(i+1, startlength, ))

        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    data_frame = pandas.DataFrame.from_records(employees)
    data_frame.to_csv('cushmanwakefield.csv')
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Add progress checking capabilities
# TODO Improve error handling
