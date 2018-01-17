import shelve
import pprint

f = shelve.open('C:\\Users\\James\\PycharmProjects\\LSBF_Scrapers\\Primitve_Scrapers\\Helper Dump\\NADCO_Proto', 'r')
start_urls = f.get('Links')
filter(None, start_urls)

pprint.pprint(start_urls)
print(len(start_urls))
