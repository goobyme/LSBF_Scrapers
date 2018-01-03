import requests
import shelve
import bs4
import os
import pprint

# os.chdir('C:\\Users\\Liberty SBF\\Desktop\\NGKF_VCF')
os.chdir("/mnt/c/Users/Liberty SBF/Desktop/NGKF_VCF")
BIGLIST = []
file = open('Offices.txt', 'r')
unformattedlist = file.readlines()
for line in unformattedlist:
    formatline = line.replace('\n', '')
    BIGLIST.append(formatline)


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

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_element = soup.find('ul', {'class': 'expanded-nav'})
    link_parent = parent_element.find('li')
    link_el = link_parent.find('a')
    link = link_el['href']

    return link

exportlist = []

for rawlink in BIGLIST:
    link = 'http://www.ngkf.com/home/about-our-firm/global-offices' + rawlink
    to_export = searchpageparsing(webdl(link))
    exportlist.append(to_export)

shelffile = shelve.open('Office_Links')
shelffile['office'] = exportlist
shelffile.close()

pprint.pprint(exportlist)
