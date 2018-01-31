from selenium import webdriver
from selenium import common
import shelve
import re
import time


initpage = "http://www.myfamp.org/view-all-directory#/"

browser = webdriver.Firefox()
browser.get(initpage)


def find_element(css_selector, element, retries=3):
    for l in range(retries):
        try:
            return element.find_element_by_css_selector(css_selector)
        except common.exceptions.NoSuchElementException:
            return None
        except common.exceptions.StaleElementReferenceException:
            continue


profile_id_regex = re.compile(r"(?<=/profile/)\d+")
links = []
stale_page = []
for i in range(366):
    time.sleep(1)
    avatar_els = browser.find_elements_by_css_selector('div.ds-avatar > img')
    for el in avatar_els:
        try:
            links.append(el.get_attribute('src'))
            print('Appended {}'.format(el.get_attribute('src')))
        except common.exceptions.StaleElementReferenceException:
            stale_page.append(i+1)
            print('Stale!'.format(i))
            continue
    time.sleep(1)
    if i != 365:
        browser.find_element_by_css_selector('a#next').click()

browser.close()

with shelve.open('FAMP_Links') as shelf_file:
    shelf_file['links'] = links
    print(stale_page)
    print('Number of stale pages'.format(len(stale_page)))
    print('Done')

