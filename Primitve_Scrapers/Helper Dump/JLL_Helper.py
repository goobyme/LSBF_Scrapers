import shelve
from selenium import webdriver
from selenium import common
import time

INITPAGE = 'http://voitco.com/our-people'   # Change this line upon failure


def core(browser):
    """Selenium scrape for prelim json data"""
    proto_profiles = []
    parent_el = browser.find_elements_by_class_name('contactItem')
    assert parent_el

    for el in parent_el:
        e = {}
        name_el = el.find_element_by_class_name("dataValue contactName")
        if name_el:
            e['Name'] = name_el.get_attribute('text')
        spec_el = el.find_element_by_class_name("dataValue specialty")
        if spec_el:
            e['Specialities'] = spec_el.get_attribute()
        if link_el:
            e['Link'] = link_el.get_attribute('href')
            e['Name'] = link_el.get_attribute('title')
        title_el = el.find_element_by_tag_name('small')
        if title_el:
            e['Department'] = title_el.text
        page_list.append(e)

    print('Parsed search-page: {}'.format(browser.current_url))
    try:
        next_page_el = browser.find_element_by_class_name('next_page')
        next_page_el.click()
    except common.exceptions.NoSuchElementException:
        return None

    return page_list


class ListLink:
    def __init__(self, sublist, passlink):
        self.sublist = sublist
        self.passlink = passlink


def iterator(start):
    sublist = []
    browser = webdriver.Firefox()
    browser.get(start)
    for i in range(10):
        time.sleep(1.5)
        to_add = core(browser)
        if to_add:
            sublist += to_add
        else:
            break
    passlink = browser.current_url
    browser.close()
    listlink_topass = ListLink(sublist, passlink)

    return listlink_topass


def main():
    nextlink = ''
    for i in range(33, 34):    # Change this line upon Failure
        if i == 33:
            linklistdata = iterator(INITPAGE)
        else:
            linklistdata = iterator(nextlink)

        nextlink = linklistdata.passlink
        shelf_file = shelve.open('JLL_Links')
        shelf_file[str(i)] = linklistdata.sublist
        shelf_file.close()
        print('Iteration {} wrote to file'.format(i+1))


if __name__ == "__main__":
    main()

# Program is designed to handle failures. Upon failure change commented lines

