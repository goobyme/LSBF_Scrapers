import scrapy


class ScottSpider(scrapy.Spider):
    name = "scottsman"
    allowed_domains = ""
    start_urls = ""

    def parse(self, response):
        self.log('Scraping {}...'.format_map(response.url))
        yield {
            response.css
        }

