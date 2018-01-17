# -*- coding: utf-8 -*-
import scrapy
import shelve
import re


class NadcoCoreSpider(scrapy.Spider):
    name = 'nadco_core'
    allowed_domains = ['http://www.nadco.org']
    f = shelve.open('/mnt/c/Users/James/PycharmProjects/LSBF_Scrapers/Primitve_Scrapers/Helper Dump/NADCO_Proto', 'r')
    start_urls = filter(None, f.get('Links'))

    def parse(self, response):
        self.log('Scraping {}'.format(response.url))
        e = {
            'LastUpdated': response.css('span.small::text').extract_first().replace('Last updated:', '').strip(),
            'FullName': response.css('b.big::text').extract_first(),
            'Email': '',
            'WorkPhone': response.css('td#tdWorkPhone::text').extract_first().strip(),
            'Company': response.css('td#tdEmployerName > a::text').extract_first().strip(),
            'City': response.css('td#tdEmployerName > a::text').extract()[1].strip(),
            'AreasOfOperation': response.css('td.CstmFldVal > a::text').extract(),
            'Associates': response.css('td#tdAssociates a::text').extract(),
            'Link': response.url
        }
        # Check for company or person using picture
        try:
            e['Classification'] = response.css('table.ViewTable1  > tr > td::text').extract()[3].strip(),
        except IndexError:
            pass
        try:
            e['State'] = response.css('td#tdEmployerName > a::text').extract()[2].strip()
        except IndexError:
            pass

        if response.css('img#MugShot::attr(src)').extract_first() == "/global_graphics/mugshot11.gif":
            e['StreetAddress'] = response.css('td#tdEmployerName::text').extract()[1].strip() + ' ' + response.css(
                'td#tdEmployerName::text').extract()[2].strip()
            e['Title/Department'] = ''
            e['Type'] = 'Firm'
        else:
            e['Title/Department'] = response.css('td#tdEmployerName::text').extract()[1].strip()
            e['StreetAddress'] = response.css('td#tdEmployerName::text').extract()[2].strip() + ' ' + response.css(
                'td#tdEmployerName::text').extract()[3].strip()
            e['Type'] = 'Employee'

        zipregex = re.compile(r"\d{5}")
        for el in response.css('td#tdEmployerName::text').extract():
            if zipregex.findall(el):
                e['PostalCode'] = el.replace('\xa0', '').replace('[', '').strip()
            else:
                continue
        return e

