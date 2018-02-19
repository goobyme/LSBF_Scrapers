from selenium import webdriver
from selenium import common
import csv
from pprint import pprint
import time

browser = webdriver.Firefox()
browser.get('https://esmba.org/page-18735')
time.sleep(1)
employees_full_list = []
val = 0

while True:
    employees_sublist = [el.get_attribute('href') for el in browser.find_elements_by_css_selector('h5 > a')]
    employees_full_list += employees_sublist
    try:
        val += 50
        browser.find_element_by_css_selector('span#idPagingData option[value="{}"]'.format(val)).click()
    except common.exceptions.NoSuchElementException:
        break

with open('ESMB.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for link in employees_full_list:
        writer.writerow([link])

pprint(employees_full_list)

