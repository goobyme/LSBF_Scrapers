import scrapy
from scrapy.crawler import CrawlerProcess
import re


class ScottSpider(scrapy.Spider):
    name = "scottsman"
    allowed_domains = ["scotsmanguide.com"]
    start_urls = ['http://www.scotsmanguide.com/Commercial/Directories/Lender/']

    def parse(self, response):
        self.log('Scraping {}...'.format(response.url))
        for link in response.css('a.DirLenderBodyText::attr(href)').extract():
            link = response.urljoin(link)
            yield scrapy.Request(url=link, callback=self.parse_details)

    def parse_details(self, response):
        self.log('Scraping Detail Page {}...'.format(response.url))
        idregex = re.compile(r"(?<==)\d$")
        lend_id = idregex.findall(response.url)[0]

        company = {
            'Company Name': response.css('td.widehalf_FWSNAME > span > strong::text').extract_first(),
            'Company Email': response.css('td.widehalf_FWSNAME > a[title="Lender Email Address"]::text').extract_first(),
            'Company Website': response.css('td.widehalf_FWSNAME > a#LenderDetail_hreWeb').extract_first(),
            'Street Address': response.css('td.widehalf_FWSNAME'),
            'Comments': response.css('td[colspan="2"]::text').extract_first(),
            'Finance URL': 'http://www.scotsmanguide.com/rsPopDefault.aspx?ucAdd=1001&MTXID=49&LenderId=' + str(lend_id)
        }
        fi_inf = scrapy.Request(url=company.setdefault('Finance URL'), callback=self.parse_finance)
        company.update(fi_inf)

        yield company

        employee_table = response.css('table.gColmCPHmain2Content1')
        for emp in employee_table.css('td.widehalf'):
            yield {
                'Name': emp.css('b::text').extract_first(),
                'Email': emp.css('a::text').extract_first(),
                'Phone': emp.css('::text').extract_first()
            }

    def parse_finance(self, response):
        loantypes = []
        property_types = []
        e_list = response.css('td.LSRbody1CwtextC')
        for i in range(4, len(e_list)-1):
            el = e_list[i]
            check = el.css('abbr::text').extract_first()
            if check == 'Y':
                if i < 19:
                    loantypes.append(el.css('abbr::attr(title').extract_first())
                else:
                    property_types.append(el.css('abbr::attr(title').extract_first())
            elif not check:
                continue

        yield {
            'Min': response.css('abbr[title="Min $"::text').extract_first(),
            'Max': response.css('abbr[title="Max $"::text').extract_first(),
            'LTV': response.css('abbr[title="LTV"::text').extract_first(),
            'DSCR': response.css('abbr[title="Debt Service Coverage Ratio"::text').extract_first(),
            'Loan Types': loantypes,
            'Property Types': property_types
        }


if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    process.crawl(ScottSpider)
    process.start()
