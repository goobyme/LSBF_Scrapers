import requests
import bs4
import re
import threading
import os
import pandas

SEARCHPAGE = 'https://www.grandbridge.com/contact'
THREADCOUNT = 10
init_proto_profile_list = []
empployees = []
llist_lock = threading.Lock()


def webdl(url):
    """Downloads web-page (using requests rather than urllib) """
    print('Downloading...{}'.format(url))
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r
    except:
        print('Download failed for {}'.format(url))
        return None


def searchpageparsing(page):
    """Scrapes search page for team links by location (can expand to search NAI globally)"""
    if not page:
        return None
    scrapelist = []
    phoneregex = re.compile(r"(?<=Phone: ).+(?= Fax:)")
    faxregex = re.compile(r"(?<=Fax: ).+")
    emailregex = re.compile(r"(?<=mailto:).+")
    notphoneregex = re.compile(r".+(?= Phone:)")

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    popop = soup.find('div', {'id': 'right_column'})
    pop = popop.find('div')
    parent_elements = pop.find_all('div')

    for i in range(2, len(parent_elements)):
        el = parent_elements[i]
        office = el.find('h2').text
        addparent = el.find('p', {'id': 'office_info'})
        text2parse = addparent.get_text().strip()
        address_strip1 = text2parse.replace('\r', '')
        address_strip2 = address_strip1.replace('\t', '')
        address_strip3 = address_strip2.replace('\n', ' ')
        phone = phoneregex.findall(address_strip3)
        fax = faxregex.findall(address_strip3)
        addressline = notphoneregex.findall(address_strip3)
        eparent = el.find('ul', {'id': 'office_employee_list'})
        emp_els = eparent.find_all('li')
        for emp in emp_els:
            e = {}
            e['Office'] = office
            if phone:
                e['Office Phone'] = phone[0]
            if fax:
                e['Fax'] = fax[0]
            if addressline:
                e['Address'] = addressline[0]
            link_el = emp.find('a')
            e['Name'] = link_el.text
            emptext = emp.contents[len(emp.contents) - 1]
            if emptext:
                emptext_strip1 = emptext.replace('\n', '')
                emptext_strip2 = emptext_strip1.replace('\t', '')
                emptext_strip3 = emptext_strip2.replace('\r', '')
                e['Title'] = emptext_strip3
            link_check = emailregex.findall(link_el['href'])
            if link_check:
                e['Email'] = link_check[0]
            else:
                e['Link'] = link_el['href']

            scrapelist.append(e)

    return scrapelist


def personparsing(page, proto_prof):
    """Parses from some combination of vcf and page and outputs list of dictionaries (iterate outside of func)"""
    try:    # Handle empty webdl failure
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return None
    e = proto_prof
    phoneregex = re.compile(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})')
    emailregex = re.compile(r'(?<=mailto:).+')

    allpara = soup.find('div', {'id': 'right_column'})
    alltext = allpara.get_text()

    phone = phoneregex.findall(alltext)
    try:
        e['Phone'] = phone[0]
    except IndexError:
        pass

    link_els = allpara.find_all('a', href=True)
    for el in link_els:
        email_check = emailregex.findall(el['href'])
        if email_check:
            try:
                e['Email'] = email_check[0]
            except IndexError:
                pass
            return e


def main():
    global init_proto_profile_list
    global employees
    employees = searchpageparsing(webdl(SEARCHPAGE))
    if employees:
        for prof in employees:
            try:
                link = 'https://www.grandbridge.com' + prof['Link']
                personparsing(webdl(link), prof)
            except KeyError:
                continue
    to_save = filter(None, employees)  # Gets rid of NoneTypes in list (very annoying if you don't do this!)
    data_frame = pandas.DataFrame.from_records(to_save)
    while True:
        try:
            data_frame.to_csv('Grandbridge.csv')
            break
        except PermissionError:
            print('Please close csv file')
            input()
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Improve progress checking capabilities
# TODO Improve error handling
