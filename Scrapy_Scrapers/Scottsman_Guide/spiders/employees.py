import scrapy


class ScottSpider(scrapy.Spider):
    name = "employees"
    allowed_domains = ["scotsmanguide.com"]
    start_urls = ['http://www.scotsmanguide.com/Residential/Directories/Lender/']

    def parse(self, response):
        self.log('Scraping {}...'.format(response.url))
        for link in response.css('a.DirLenderBodyText::attr(href)').extract():
            link = response.urljoin(link)
            yield scrapy.Request(url=link, callback=self.parse_details)

    def parse_details(self, response):
        self.log('Scraping Detail Page {}...'.format(response.url))

        for emp_el in response.css('tr > td[colspan="2"] > table.gColmCPHmain2Content1 td.widehalf'):
            x = emp_el.css('td.widehalf::text').extract()
            if len(x) == 2:
                title = x[0]
                phone = x[1]
            elif len(x) == 3:
                title = x[1]
                phone = x[2]
            else:
                continue
            yield {
                'Name': emp_el.css('b::text').extract_first(),
                'Email': emp_el.css('a::text').extract_first(),
                'Phone': phone,
                'Title': title,
                'Company': response.css('td.widehalf_FWSNAME > span > strong::text').extract_first()
            }

