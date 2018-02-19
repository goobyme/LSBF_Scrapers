# -*- coding: utf-8 -*-
import scrapy


class NyscarCoreSpider(scrapy.Spider):
    name = 'nyscar_core'
    allowed_domains = ['nyscar.org']
    start_urls = ['https://www.nyscar.org/directory']

    def parse(self, response):
        pass
