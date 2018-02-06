from selenium import webdriver
from selenium import common
import shelve
import time


initpage = "http://www.myfamp.org/directory#/"

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


def selenium_threadbot():
    pass


def main():
    pass

links = []
page_count = browser.find_element_by_css_selector('strong#total-pages').text
for i in range(int(page_count)):
    profile_elements_count = len(browser.find_elements_by_css_selector('div.search-profile.ng-scope'))
    for x in range(profile_elements_count):
        profile_elements = browser.find_elements_by_css_selector('div.search-profile.ng-scope')
        profile_elements[x].click()
        time.sleep(2)
        links.append(browser.current_url)
        print('Appended {}'.format(browser.current_url))
        find_element('a#back-to-search', browser).click()
        time.sleep(2)
    if i != 93:
        browser.find_element_by_css_selector('a#next').click()


with shelve.open('FAMP_Links') as shelf_file:
    shelf_file['links'] = links

if '__name__' == '__main__':
    main()
