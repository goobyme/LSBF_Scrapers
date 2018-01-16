import shelve
from selenium import webdriver
import pprint as pp

INITPAGE = 'http://www.nadco.org/searchserver/people.aspx?id=D2F7175D-A978-484A-A0F9-8F34D6CC6458&cdbid=&canconnect=0&canmessage=0&map=False&toggle=False&hhSearchTerms='   # Change this line upon failure


def core(browser):
    """Selenium scrape for prelim json data"""
    profile_links = []
    for el in browser.find_elements_by_css_selector('td[style="width:75%;border:none;"] a'):
        profile_links.append(el.get_attribute('href'))

    return profile_links


def iterator(start):
    sublist = []
    browser = webdriver.Chrome()
    browser.get(start)
    for i in range(1, 40):
        print('Scraping page {}'.format(i))
        sublist += core(browser)
        parent_pagination = (browser.find_element_by_css_selector('td[style="font-size:9pt;"]'))
        if i == 40:
            break
        elif i % 10 == 0:
            parent_pagination.find_element_by_css_selector('img[src="/global_graphics/icons/pageRight.gif"]').click()
        else:
            parent_pagination.find_element_by_link_text(str(i+1)).click()

    browser.close()
    return sublist


def main():
    proto_profiles = iterator(INITPAGE)

    shelf_file = shelve.open('NADCO_Proto')
    shelf_file['Links'] = proto_profiles
    shelf_file.close()
    print('Wrote to file\n')
    pp.pprint(proto_profiles)


if __name__ == "__main__":
    main()
