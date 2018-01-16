import scrapy


class ScottSpider(scrapy.Spider):
    name = "lendata"
    allowed_domains = ["scotsmanguide.com"]
    start_urls = ['http://www.scotsmanguide.com/Residential/Directories/Lender/']

    def parse(self, response):
        self.log('Scraping {}...'.format(response.url))
        for link in response.css('a.DirLenderBodyText::attr(href)').extract():
            link = response.urljoin(link)
            yield {'link': link}
