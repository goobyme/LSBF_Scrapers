# -*- coding: utf-8 -*-
import scrapy
import re
import shelve


class ProfileSpider(scrapy.Spider):
    name = 'profile'
    allowed_domains = ['myfamp.org']
    start_urls = []
    with shelve.open(
            '/mnt/c/Users/James/PycharmProjects/LSBF_Scrapers/Primitve_Scrapers/Helper Dump/FAMP_Links') as shelf_file:
        profile_id_regex = re.compile(r"(?<=/profile/)\d+")
        for raw_text in shelf_file['links']:
            start_urls.append(profile_id_regex.findall(raw_text)[0])

    def parse(self, response):
        e = {
            'FullName': response.css('div.profile-info > div.contentheading::text').extract_first(),
            'MemberType': response.css('div[data-type="12"]::text').extract_first(),
            'Group': response.css('div[data-type="13"]::text').extract_first(),
            'MemberStatus': response.css('div[data-type="13"]::text').extract_first(),
            'Organization': response.css('div[data-type="5"]::text').extract_first(),
            'Email_Main': response.css('div[data-type="2"] > a::text').extract_first(),
            'Phone_Main': response.css('div[data-type="4"]::text').extract_first(),
            'Fax': response.css('div[data-cid="282"]::text').extract_first(),
        }

        cityregex = re.compile(r"^.+(?= )")
        stateregex = re.compile(r"[A-Z]{2}")
        zipregex = re.compile(r"\d{5}$")

        address_list = response.css('div[data-type="3"]::text').extract_first()
        for i, address_line in enumerate(address_list):
            if i == 0:
                e['StreetAddress'] = address_line
            if i == 2:
                e['Country'] = address_line
            if i == 1:
                try:
                    e['State'] = stateregex.findall(address_line)[0]
                except IndexError:
                    pass
                try:
                    e['PostalCode'] = zipregex.findall(address_line)[0]
                except IndexError:
                    pass
                try:
                    e['City'] = cityregex.findall(address_line)[0]
                except IndexError:
                    pass
            else:
                continue

        for el in response.css('div#customs > div.attribute').extract():
            field = el.css('label > strong::text').extract_one()
            entry = el.css('::text').extract_one()
            try:
                e[field] = entry
            except TypeError:
                continue

        return e
