import shelve
from selenium import webdriver
from selenium import common
import time
import pprint as pp

INITPAGE = 'https://www.stanjohnsonco.com/our-people'   # Change this line upon failure


def core(browser):
    """Selenium scrape for prelim json data"""
    proto_profiles = []
    parent_el = browser.find_elements_by_class_name('views-field.views-field-nothing')
    assert parent_el

    for el in parent_el:
        e = {}
        try:
            name_el = el.find_element_by_class_name("kw-userFullname")
            e['Name'] = name_el.text
        except common.exceptions.NoSuchElementException:
            print('[Error-Core]: Name Element not found')
        try:
            title_el = el.find_element_by_class_name("kw-userPosition")
            e['Title'] = title_el.text
        except common.exceptions.NoSuchElementException:
            print('[Warning-Core]: Title Element not found')
        try:
            adr_el = el.find_element_by_class_name('kw-userAddress')
            e['City/State'] = adr_el.text
        except common.exceptions.NoSuchElementException:
            print('[Warning-Core]: Address Element not found')
        try:
            link_el = el.find_element_by_tag_name('a')
            e['Link'] = link_el.get_attribute('href')
        except common.exceptions.NoSuchElementException:
            print('[Error-Core]: Link Element not found')

        proto_profiles.append(e)
    print('Parsed search-page')
    try:
        next_page = browser.find_element_by_class_name('pager-next.last')
        next_page.click()
        gonogo = True
    except common.exceptions.NoSuchElementException:
        try:
            next_page = browser.find_element_by_class_name('pager-next.first.last')
            next_page.click()
            gonogo = True
        except common.exceptions.NoSuchElementException:
            gonogo = False

    returnobj = ListLink(proto_profiles, gonogo)
    return returnobj


class ListLink:
    def __init__(self, protoprof, gonogo):
        self.protoprof = protoprof
        self.gonogo = gonogo


def iterator(start):
    sublist = []
    browser = webdriver.Firefox()
    browser.get(start)
    while True:
        time.sleep(3)
        to_add = core(browser)
        if to_add.gonogo:
            sublist += to_add.protoprof
        else:
            sublist += to_add.protoprof
            break

    browser.close()
    return sublist


def main():
    proto_profiles = iterator(INITPAGE)

    shelf_file = shelve.open('StanJohnson_Links')
    shelf_file['Profiles'] = proto_profiles
    shelf_file.close()
    print('Wrote to file\n')
    pp.pprint(proto_profiles)


if __name__ == "__main__":
    main()
