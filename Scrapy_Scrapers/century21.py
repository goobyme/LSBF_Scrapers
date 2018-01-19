import scrapy
from Reference import StateCodesList
import json
from bs4 import BeautifulSoup
import re


class Cent21Spider(scrapy.Spider):
    name = 'century21'
    allowed_domains = ['http://www.nadco.org']
    start_urls = []
    for state in StateCodesList.codes:
        start_urls.append(
            'https://commercial.century21.com/search.c21?lid=S{}&t=2&o=&s=0&subView=searchView.Paginate'.format(state))

    def parse(self, response):
        data = json.loads(response.body)
        html_str = data['list']
        soup = BeautifulSoup(html_str, 'lxml')
        for link in soup.find_all('h4', {'class': 'centerLaneCardHeader agent'}):
            yield scrapy.Requests(url=response.urljoin(link), callback=self.prof_parse)

        pagenumberregex = re.compile(r"(?<=&s=)\d+")
        pagenumberregex.sub(response.url, str(int(pagenumberregex.findall(response.url)[0]) + 10))
        yield scrapy.Requests(
            url=pagenumberregex.sub(response.url, str(int(pagenumberregex.findall(response.url)[0]) + 10)),
            callback=self.parse)

    def prof_parse(self):
        pass
