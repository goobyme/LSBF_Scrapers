from selenium import webdriver
from selenium.common import exceptions
import csv
import pandas


def core(browser):
    """Selenium scrape for prelim json data"""
    profile_links = []
    for el in browser.find_elements_by_css_selector('td[style="width:75%;border:none;"] a'):
        profile_links.append(el.get_attribute('href'))

    return profile_links


def iterator(start):
    browser = webdriver.Chrome()
    browser.get(start)
    email_el = browser.find_element_by_css_selector('td > script ~ a')
    email_text = email_el.text

    return email_text


def main():
    new_csv = []
    with open('C:\\Users\\James\\PycharmProjects\\LSBF_Scrapers\\Scrapy_Scrapers\\NADCO\\nadcodata_7.csv',
              newline='') as csvfile:
        dictreader = csv.DictReader(csvfile)
        browser = webdriver.Chrome()
        i = 1
        for row in dictreader:
            browser.get(row['Link'])
            try:
                email_el = browser.find_element_by_css_selector('td > script ~ a')
                email_text = email_el.text
                row['Email'] = email_text
                print('Scraped {} {} of 941'.format(email_text, i))
            except exceptions.NoSuchElementException or exceptions.StaleElementReferenceException:
                print('Failed to scrape page {} of 941'.format(i))
            new_csv.append(row)
            i += 1
        browser.close()
        csvfile.close()

    dataframe = pandas.DataFrame.from_records(new_csv)
    dataframe.to_csv('nadcoemail.csv')

if __name__ == "__main__":
    main()
