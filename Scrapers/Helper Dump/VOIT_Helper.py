import shelve
from selenium import webdriver
from selenium import common
import time
import pprint as pp

INITPAGE = 'http://voitco.com/our-people'   # Change this line upon failure


def core(browser, pageno):
    """Selenium scrape for prelim json data"""
    proto_profiles = []
    parent_el = browser.find_elements_by_class_name('contactItem')
    assert parent_el

    for el in parent_el:
        e = {}
        try:
            name_el = el.find_element_by_class_name("contactName")
            e['Name'] = name_el.text
        except common.exceptions.NoSuchElementException:
            print('[Error-Core]: Name Element not found')
        try:
            spec_el = el.find_element_by_class_name("specialty")
            e['Specialities'] = spec_el.text
        except common.exceptions.NoSuchElementException:
            print('[Warning-Core]: Specialty Element not found')
        try:
            vcard_el = el.find_element_by_class_name('vcard')
            e['VCard'] = vcard_el.get_attribute('href')
        except common.exceptions.NoSuchElementException:
            print('[Error-Core]: VCard Element not found')
        try:
            resume_el = el.find_element_by_class_name('resume')
            e['Resume'] = resume_el.get_attribute('href')
        except common.exceptions.NoSuchElementException:
            print('[Logging-Core]: Resume Element not found')

        proto_profiles.append(e)

    print('Parsed search-page: {}'.format(pageno))
    if pageno < 9:
        try:
            next_page_el = browser.find_element_by_id(
                'ctl00_ContentPlaceHolder4_C004_rlvResults_RadDataPager1_ctl02_NextButton')
            next_page_el.click()
        except common.exceptions.NoSuchElementException:
            return None
    elif pageno == 9:
        try:
            next_page_el = browser.find_element_by_id(
                'ctl00_ContentPlaceHolder4_C004_rlvResults_RadDataPager1_ctl02_LastButton')
            next_page_el.click()
        except common.exceptions.NoSuchElementException:
            return None
    else:
        try:
            next_page_el = browser.find_element_by_id(
                'ctl00_ContentPlaceHolder4_C004_rlvResults_RadDataPager1_ctl00_PrevButton')
            next_page_el.click()
        except common.exceptions.NoSuchElementException:
            return None

    return proto_profiles


class ListLink:
    def __init__(self, sublist, passlink):
        self.sublist = sublist
        self.passlink = passlink


def iterator(start):
    sublist = []
    browser = webdriver.Firefox()
    browser.get(start)
    for i in range(12):
        if i != 0:
            time.sleep(10)
        to_add = core(browser, i+1)
        if to_add:
            sublist += to_add
        else:
            break

    browser.close()
    return sublist


def main():
    proto_profiles = iterator(INITPAGE)

    shelf_file = shelve.open('VOIT_Links')
    shelf_file['Profiles'] = proto_profiles
    shelf_file.close()
    print('Wrote to file\n')
    pp.pprint(proto_profiles)


if __name__ == "__main__":
    main()
