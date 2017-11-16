import requests
import bs4
import re
import threading
import os


SEARCHPAGE = 'https://www.hfflp.com/our-people/search.aspx?q=*'
THREADCOUNT = 10
link_list = []
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


def searchpageparsing(page):
    """Scrapes search page for team links by location (can expand to search NAI globally)"""
    scrapelist = []

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_elements = soup.find_all('div', {'class':'media-body'})

    for element in parent_elements:
        link_element = element.find('a', {'class':'media-link-vcard'})
        link = link_element['href']
        scrapelist.append(link)

    return scrapelist


def personparsing(page):

    es = []
    element = bs4.BeautifulSoup(page.text, 'lxml')

    phoneregex = re.compile(r"(?<=Phone: ).+(?=\n)")
    faxregex = re.compile(r"(?<=Fax: ).+(?=\n)")

    e = {}

    name = element.find_all('h1')[0]
    e["Name"] = name.get_text()

    title = element.find_all('h2')[0]
    e['Title'] = title.get_text()

    email_parent = element.find_all('span', {'class': 'email'})
    email = email_parent[0].find_all('a')
    e["Email"] = email[0].get_text()

    loc_parent = element.find_all('div', {'class': 'primary-location'})
    loc = loc_parent[0].find_all('a')
    location = loc[0].get_text()
    e["Location"] = location.strip()

    phone_parent = element.find_all('div', {'class': 'locations'})
    phone_text = phone_parent[0].get_text()
    phone = phoneregex.findall(phone_text)
    try:
        e["Phone"] = phone[0]
    except IndexError:
        pass

    fax = faxregex.findall(phone_text)
    try:
        e['Fax'] = fax[0]
    except IndexError:
        pass

    details = element.find_all('div', {'class': 'detail-column'})

    if details:
        edu_elements = details[0].find_all('li')
        education = ''
        for i in edu_elements:
            part = i.get_text()
            education += part + ', '
        e['Education'] = education

        if len(details) > 1:
            aff_elements = details[1].find_all('li')
            aff = ''
            for i in aff_elements:
                part = i.get_text()
                aff += part + ', '
            e['Affiliations'] = aff.replace('\n', '')

    es.append(e)
    return es


def threadbot(ident, total_len):

    i = 1
    while True:
        llist_lock.acquire()
        if len(link_list) > 0:
            try:
                length = len(link_list)
                link = link_list[0]
                link_list.remove(link)
            finally:
                llist_lock.release()
            print('Thread %s downloading page %s of %s' % (ident, total_len-length, total_len))
            to_dl = webdl(link)
            vcf_file = open('hff_vcf_%s_%s.vcf' % (ident, i), 'wb')
            for chunk in to_dl.iter_content(100000):
                vcf_file.write(chunk)
            vcf_file.close()
            i += 1
        else:
            llist_lock.release()
            print('Thread %s completed parsing' % ident)
            break


def main():

    os.chdir('/mnt/c/Users/Liberty SBF/Desktop/HFF_VCF')

    global link_list
    link_list = searchpageparsing(webdl(SEARCHPAGE))
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
