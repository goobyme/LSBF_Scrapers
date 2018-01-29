import csv
import requests
import bs4
import re
import pandas
import threading

csv_lock = threading.Lock()
row_lock = threading.Lock()
THREAD_COUNT = 25
rowlist = []
new_csv = []


def scraper_core(row):
    global new_csv
    scraperegex = re.compile(r"\.c21\.com$")
    emailregex = re.compile(r"(?<=Mail: ).+@\S+(?=\s)")

    def write_to_list(fail=True, email=''):
        row_count = 6792
        csv_lock.acquire()
        try:
            new_csv.append(row)
            if fail:
                print('Failed to Scrape {} of {}'.format(len(new_csv), row_count))
            else:
                print('Scraped {} {} of {}'.format(email, len(new_csv), row_count))
        finally:
            csv_lock.release()

    if not scraperegex.findall(row.setdefault('ProfilePage', '')):
        write_to_list()
        return None
    try:
        r = requests.get(row['ProfilePage'])
        r.raise_for_status()
    except Exception:
        write_to_list()
        return None
    soup = bs4.BeautifulSoup(r.text, 'lxml')

    raw_text = soup.select_one('div.footer-col-contact').get_text()
    if emailregex.findall(raw_text):
        row['Email'] = emailregex.findall(raw_text)[0]
        write_to_list(fail=False, email=row['Email'])
    else:
        write_to_list()


def threadbot():
    global rowlist
    row = None
    while True:
        row_lock.acquire()
        try:
            if len(rowlist) != 0:
                row = rowlist[0]
                rowlist.remove(row)
            else:
                break
        finally:
            row_lock.release()

        scraper_core(row)


def main():
    global rowlist

    with open('C:\\Users\\Liberty SBF\\PycharmProjects\\LSBF_Scrapers\\Primitve_Scrapers\\C21\\Century21_NoEmails-1-raw.csv'
            ,newline='') as csvfile:
        dictreader = csv.DictReader(csvfile)
        for row in dictreader:
            rowlist.append(row)

    thread_list = []
    for i in range(THREAD_COUNT):
        thread = threading.Thread(target=threadbot, args=())
        thread_list.append(thread)
        thread.start()

    for thread in thread_list:
        thread.join()
    dataframe = pandas.DataFrame.from_records(new_csv)
    dataframe.to_csv('c21email.csv')
    print('Done')


if __name__ == '__main__':
    main()
