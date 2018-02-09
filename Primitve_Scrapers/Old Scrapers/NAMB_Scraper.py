from selenium import webdriver
from selenium.common import exceptions
import pandas
import re
import time

initpage = "https://mms.namb.org/members/directory/search_namb.php"

browser = webdriver.Chrome()
browser.get(initpage)

browser.find_element_by_css_selector('input#search_button').click()
time.sleep(2)

profiles = []
x = 1
while True:
    for profile_el in browser.find_elements_by_css_selector('div.open-div'):
        e = {}
        for i in range(3):
            try:
                e['FullName']= profile_el.find_element_by_css_selector('div.name-plate').text
                break
            except exceptions.StaleElementReferenceException:
                continue
        for i in range(3):
            try:
                e['WorkPhone'] = profile_el.find_element_by_css_selector('div.phone-number').text.replace('Phone: ', '')
                break
            except exceptions.NoSuchElementException:
                pass
            except exceptions.StaleElementReferenceException:
                continue
        for i in range(3):
            try:
                e['Fax'] = profile_el.find_element_by_css_selector('div.fax-number').text.replace('Fax: ', '')
                break
            except exceptions.NoSuchElementException:
                pass
            except exceptions.StaleElementReferenceException:
                continue

        def address_parser(text):
            cityregex = re.compile(r"^.+,")
            stateregex = re.compile(r"[A-Z]{2}")
            zipregex = re.compile(r"\d{5}$")

            if cityregex.findall(text):
                e['City'] = cityregex.findall(text)[0]
            if stateregex.findall(text):
                e['State'] = stateregex.findall(text)[0]
            if zipregex.findall(text):
                e['PostalCode'] = zipregex.findall(text)[0]

        for i in range(3):
            try:
                address_text = profile_el.find_element_by_css_selector('div.address-block').text.split('\n')
                if len(address_text) == 3:
                    e['StreetAddress'] = address_text[0] + ' ' + address_text[1]
                    address_parser(address_text[2])
                elif len(address_text) == 2:
                    e['StreetAddress'] = address_text[0]
                    address_parser(address_text[1])
                break
            except exceptions.StaleElementReferenceException:
                continue
            except exceptions.NoSuchElementException:
                pass
        for i in range(3):
            try:
                text_list = profile_el.find_element_by_css_selector('div.member-info').text.split('\n')
                e['Company'] = text_list[1]
                if text_list[2]:
                    e['Title'] = text_list[2]
                break
            except exceptions.StaleElementReferenceException:
                continue
        for i in range(3):
            try:
                link_els = profile_el.find_elements_by_css_selector('div.icon-display > a')
                for el in link_els:
                    try:
                        link = el.get_attribute('href')
                        if 'mcontact' in link:
                            e['ContactForm'] = 'https://mms.namb.org/' + link
                        else:
                            e['Websiste'] = link
                    except exceptions.NoSuchAttributeException:
                        continue
                break
            except exceptions.StaleElementReferenceException:
                continue

        for i in range(3):
            try:
                for el in profile_el.find_elements_by_css_selector('div.additional-info'):
                    try:
                        field = el.find_element_by_css_selector('div').text
                        entry = el.text.replace(field, '').strip()
                        e[field] = entry
                    except exceptions.StaleElementReferenceException:
                        continue
                break
            except exceptions.StaleElementReferenceException:
                continue
        profiles.append(e)

    try:
        print('Parsed page {}'.format(x))
        x += 1
        browser.find_element_by_css_selector('div.pagination-button.page-number.next-page').click()
        time.sleep(2)
        continue
    except exceptions.NoSuchElementException:
        break

browser.close()

dataframe = pandas.DataFrame.from_records(profiles)
dataframe.to_csv('NAMB_2.csv')
print('Done')
