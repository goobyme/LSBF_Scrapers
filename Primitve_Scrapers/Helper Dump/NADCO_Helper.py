import shelve
from selenium import webdriver
from selenium import common
from selenium.webdriver.support.ui import Select
import time
import pprint as pp

INITPAGE = 'http://www.nadco.org/search/custom.asp?id=1316'   # Change this line upon failure


def core(browser):
    """Selenium scrape for prelim json data"""
    profile_links = []
    for el in browser.find_element_by_css_selector('td[style="width:75%;border:none;"] a'):
	      profile_links.append(el['href'])

    return profile_links


def iterator(start):
    sublist = []
    browser = webdriver.Chrome()
    browser.get(start)
    for state in browser.find_elements_by_css_selector('option:'):
        select = Select(browser.find_element_by_css_selector(
        'select[name="cdlCustomFieldValueIDSTATES-SINGLE"]'))
        select.select_by_visible_text(state.text)
        browser.find_element_by_css_selector('input[type="submit"]').click()
        sublist += core(browser)
        browser.back()

    browser.close()
    return sublist


def main():
    proto_profiles = iterator(INITPAGE)

    shelf_file = shelve.open('NADCO_Proto')
    shelf_file['Profiles'] = proto_profiles
    shelf_file.close()
    print('Wrote to file\n')
    pp.pprint(proto_profiles)


if __name__ == "__main__":
    main()
