# -*- coding: utf-8 -*-
import scrapy
import re


class ZipsearcherSpider(scrapy.Spider):
    name = 'zipsearcher'
    allowed_domains = ['realtor.com', 'credemographics.com']
    start_urls = ['http://www.credemographics.com/demographics/top-1000-u.s.-zip-codes']

    # def __init__(self, range_start=0, range_end=int(), *args, **kwargs):
    #     super(ZipsearcherSpider, self).__init__(*args, **kwargs)
    #     self.range_start = range_start
    #     self.range_end = range_end

    def parse(self, response):
        for zipcode in response.css('td.text-center > a.zip::text').extract()[401:500]:
            yield scrapy.Request(
                url='https://www.realtor.com/realestateagents/{}/pg-1'.format(zipcode), callback=self.paginate)

    def paginate(self, response):
        if response.css('div.agent-list-card.clearfix'):
            for profile in response.css('div.agent-list-card.clearfix'):
                yield {
                    'FullName': profile.css('div.agent-name.text-bold > a::text').extract_first().strip(),
                    'Link': response.urljoin(profile.css('div.agent-name.text-bold > a::attr(href)').extract_first())
                }
            page_number_regex = re.compile(r"(?<=pg-)\d+$")
            page_number = int(page_number_regex.findall(response.url)[0])
            yield scrapy.Request(url=page_number_regex.sub(str(page_number+1), response.url), callback=self.paginate)

