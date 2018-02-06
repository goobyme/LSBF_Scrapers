# -*- coding: utf-8 -*-
import scrapy
import csv


class ProfileParserSpider(scrapy.Spider):
    name = 'profile_Parser'
    allowed_domains = ['realtor.com']
    start_urls = []
    with open('/mnt/c/Users/James/PycharmProjects/LSBF_Scrapers/Scrapy_Scrapers/Realtor/realtor_profiles.csv',
              'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            start_urls.append(row.setdefault('Link', ''))

    def parse(self, response):
        e = dict(Link=response.url)

        e['FullName'] = response.css('div.title-row.clearfix > h2 > span[itemprop="name"]::text').extract_first()
        e['YearsExperience'] = response.css('ul.plain-list span.list-label ~ span::text').extract_first()
        e['AreasServiced'] = response.css('span#area_served_set::text').extract_first().strip()
        e['Speicalizations'] = response.css('span#specialization_set::text').extract_first().strip()
        e['PriceRange'] = response.css('span[itemprop="priceRange"] > span::text').extract_first()\
            .replace('\t', '').replace('\n','')
        e['Website'] = response.css('a[data-omtag="adp:contact-info:site"]::attr(href)').extract_first()

        for number, phone in enumerate(response.css('span[itemprop="telephone"]::text').extract()):
            e['Phone_{}'.format(number)] = phone
        cert_list = []
        for cert in response.css('div.certification-list i::attr(class)').extract():
            if cert == 'icon-certification-gri':
                cert_list.append('REALTOR Institute Graduate')
            if cert == 'icon-certification-sfr':
                cert_list.append('Short-Sales & Foreclosure')
        e['Certifications'] = cert_list
        e['Company'] = response.css('h4.margin-top-sm::text').extract_first()

        address_raw = response.css('span[itemprop="streetAddress"]::text').extract_first()
        address_list = address_raw.split(', ')

        def address_writer(length_code):
            new_code = length_code - 4  # 0 for length of 4, 1 for length 5 (double street line)
            street_add_list = []
            for number, item in enumerate(address_list):
                if number == 0 and new_code == 0:
                    e['StreetAddress'] = item
                elif number == 0 and new_code == 1:
                    street_add_list.append(item)
                elif number == 1 and new_code == 1:
                    street_add_list.append(item)
                    e['StreetAddress'] = ' '.join(street_add_list)
                elif number == 1 + new_code:
                    e['City'] = item
                elif number == 2 + new_code:
                    e['State'] = item
                elif number == 3 + new_code:
                    e['PostalCode'] = item

        if len(address_list) in range(4, 5):
            address_writer(len(address_list))

        yield e
