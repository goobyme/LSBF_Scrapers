# -*- coding: utf-8 -*-
import scrapy
import re


class CoreSpider(scrapy.Spider):
    name = 'core'
    allowed_domains = ['californiamortgageassociation.org']
    start_urls = ['http://californiamortgageassociation.org/member-directory/?start=0&where=WHERE+tblPeople.CompanyID+%3D+tblCompany.CompanyID+AND+%28tblPeople.MemberStatus+%3D+%27REGULAR%27+OR+tblPeople.MemberStatus+%3D+%27Affiliate%27+OR+tblPeople.MemberStatus+%3D+%27Educational%27+OR+tblPeople.MemberStatus+%3D+%27Additional+Member%27%29+AND+%28tblPeople.flgOptOutDirectory+%3D+0%29&type=query']

    def parse(self, response):
        e = dict()
        el_list = list(response.css('table#searchresults > tr > td'))
        name_list = response.css('table#searchresults > tr > td > span.bold::text').extract()
        namecount = 0

        fieldregex = re.compile(r'.+:$')
        cityregex = re.compile(r"^.+(?=,)")
        stateregex = re.compile(r"[A-Z]{2}")
        zipregex = re.compile(r"\d{5}$")

        for number, line in enumerate(el_list):
            line_text = line.css('td::text').extract_first()
            prev_text = ''
            if not type(line_text) == str:
                line_text = line.css('a::text').extract_first()
                if not type(line_text) == str:
                    continue
            if number != 0:
                prev_text = el_list[number-1].css('td::text').extract_first()
                if not prev_text:
                    prev_text = ''
            if line_text == 'Name:' and number != 0:
                yield e
                e.clear()
                e[line_text] = name_list[namecount]
                namecount += 1
                continue
            elif fieldregex.findall(line_text):
                e[line_text] = ''
            elif fieldregex.findall(prev_text):
                if prev_text == "Name:":
                    print('Name skip')
                    continue
                elif prev_text == "Website:" or "Email:":
                    e[prev_text] = line.css('a::text').extract_first()
                else:
                    print('THIS IS LINE TEXT {}'.format(line_text))
                    e[prev_text] = line_text
            else:
                try:
                    e['City'] = cityregex.findall(line_text)[0].replace(', CA', '')
                    e['State'] = stateregex.findall(line_text)[0]
                    e['PostalCode'] = zipregex.findall(line_text)[0]
                except IndexError:
                    pass

        pagenoregex = re.compile(r'(?<=\?start=)[0-9]+')
        new_page_number = int(pagenoregex.findall(response.url)[0]) + 20
        new_url = pagenoregex.sub(str(new_page_number), response.url)
        if el_list:
            yield scrapy.Request(url=new_url, callback=self.parse)

