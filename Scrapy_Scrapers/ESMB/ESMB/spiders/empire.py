# -*- coding: utf-8 -*-
import scrapy
import csv


class EmpireSpider(scrapy.Spider):
    name = 'empire'
    allowed_domains = ['esmba.org']
    with open('/mnt/c/Users/James/PycharmProjects/LSBF_Scrapers/Primitve_Scrapers/ESMB.csv',
              'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        start_urls = [''.join(row) for row in reader]

    def parse(self, response):
        e = dict()

        e['MembershipStatus'] = response.css('div#idMembershipLevelContainer div.fieldBody > span::text'
                                             ).extract_first()
        e['FirstName'] = response.css(
            'span#FunctionalBlock1_ctl00_ctl00_memberProfile_MemberForm_memberFormRepeater_ctl00_TextBoxLabel7703799::text'
        ).extract_first()

        e['LastName'] = response.css(
            'span#FunctionalBlock1_ctl00_ctl00_memberProfile_MemberForm_memberFormRepeater_ctl01_TextBoxLabel7703800::text'
        ).extract_first()

        e['Company'] = response.css('div#idContainer7703801 div.fieldBody > span::text').extract_first()
        e['Title'] = response.css('div#idContainer7711068 div.fieldBody > span::text').extract_first()
        e['StreetAddress'] = response.css('div#idContainer7703855 div.fieldBody > span::text').extract_first()
        e['City'] = response.css('div#idContainer7703856 div.fieldBody > span::text').extract_first()
        e['State'] = response.css('div#idContainer7703857 div.fieldBody > span::text').extract_first()
        e['PostalCode'] = response.css('div#idContainer7703858 div.fieldBody > span::text').extract_first()
        e['Phone'] = response.css('div#idContainer7703804 div.fieldBody > span::text').extract_first()
        e['Email'] = response.css('div#idContainer7703798 div.fieldBody > span > a::text').extract_first()

        e['Associations'] = response.css('a.bundlContact::text').extract()

        yield e
