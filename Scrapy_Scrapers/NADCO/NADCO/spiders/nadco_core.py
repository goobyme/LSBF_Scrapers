# -*- coding: utf-8 -*-
import scrapy
import shelve


class NadcoCoreSpider(scrapy.Spider):
    name = 'nadco_core'
    allowed_domains = ['http://www.nadco.org']
    f = shelve.open('/mnt/c/Users/James/PycharmProjects/LSBF_Scrapers/Primitive_Scrapers/Helper Dump/NADCO_Proto', 'r')
    start_urls = f.get('Links')

    def parse(self, response):
        self.log('Scraping {}'.format(response.url))
        yield {
            'LastUpdated': response.css('span.small::text').extract_first().replace('Last updated:', '').strip(),
            'FullName': response.css('b.big::text').extract_first(),
            'Classification': '',
            'Title': response.css('td#tdEmployerName::text').extract()[1].strip(),
            'Email': response.css('tbody > tr > td > a::attr(href)').extract_first(),
            'WorkPhone': response.css('td#tdWorkPhone::text').extract_first().strip(),
            'Company': response.css('td#tdEmployerName > a::text').extract_first().strip(),
            'StreetAddress': response.css('td#tdEmployerName::text').extract()[2].strip() + ' ' + response.css(
                            'td#tdEmployerName::text').extract()[3].strip(),
            'City': response.css('td#tdEmployerName > a::text').extract().strip()[1],
            'State': response.css('td#tdEmployerName > a::text').extract().strip()[2],
            'PostalCode': response.css('td#tdEmployerName::text').extract()[6].strip(),
            'AreasofOperation': response.css('td.CstmFldVal > a::text').extract(),
            'Associates': response.css('td#tdAssociates a::text').extract(),
            'Link': response.url
        }
