import scrapy
import re
import csv


class ScottSpider(scrapy.Spider):
    name = 'findata'
    allowed_domains = ["scotsmanguide.com"]
    start_urls = []
    with open('/mnt/c/Users/James/PycharmProjects/LSBF_Scrapers/Scrapy_Scrapers/lendata.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            start_urls.append(row['link'])

    def parse(self, response):
        self.log('Scraping Detail Page {}...'.format(response.url))
        idregex = re.compile(r"(?<==)\d+$")
        lend_id = idregex.findall(response.url)[0]
        to_splice = response.css('td.widehalf_FWSNAME::text').extract()
        spliced = to_splice[1].split(',')

        company = {
            'Company Name': response.css('td.widehalf_FWSNAME > span > strong::text').extract_first(),
            'Company Email': response.css('td.widehalf_FWSNAME > a[title="Lender Email Address"]::text').extract_first(),
            'Company Website': response.css('td.widehalf_FWSNAME > a#LenderDetail_hreWeb::attr(href)').extract_first(),
            'Street Address': to_splice[0],
            'Comments': response.css('td[colspan="2"]::text').extract()[2],
            'City': spliced[0].strip(),
            'State': spliced[1].strip()
        }
        try:
            company['Company Phone'] = to_splice[2].replace('Phone:', '')
        except IndexError:
            pass
        try:
            company['Postal Code'] = spliced[2].strip()
        except IndexError:
            pass

        matrix_id = response.css('img.imMatrix::attr(title)').extract()
        ref = {'Commercial': '49', 'Hard Money':'51', 'Multifamily': '50', 'Construction': '52'}
        url_list = []
        for item in matrix_id:
            parse_id = ref.setdefault(item, '49')
            url_list.append("""http://www.scotsmanguide.com/rsPopDefault.aspx?ucAdd=1001&MTXID={}&LenderId={}""".format(
                parse_id, lend_id))

        company['Finance URL'] = url_list

        req = scrapy.Request(url=url_list[0], callback=self.parse_finance)
        req.meta['company'] = company
        yield req

    def parse_finance(self, response):
        self.log('Scraping{}'.format(response.url))
        loantypes = []
        property_types = []
        e_list = response.css('td.LSRbody1CwtextC')
        for i in range(4, len(e_list)-1):
            el = e_list[i]
            check = el.css('abbr::text').extract_first()
            if check == 'Y':
                if i < 19:
                    loantypes.append(el.css('abbr::attr(title)').extract_first())
                else:
                    property_types.append(el.css('abbr::attr(title)').extract_first())
            elif not check:
                continue
        fin_dat = {
            'Min': response.css('abbr[title="Min $"]::text').extract_first(),
            'Max': response.css('abbr[title="Max $"]::text').extract_first(),
            'LTV': response.css('abbr[title="LTV"]::text').extract_first(),
            'DSCR': response.css('abbr[title="Debt Service Coverage Ratio"]::text').extract_first(),
            'Firm Type': response.css('td[colspan="4"]::text').extract_first(),
            "Covered Regions": response.css('td[colspan="34"] > span::text').extract_first(),
            'Loan Types': loantypes,
            'Property Types': property_types
        }
        company = response.meta['company']
        company.update(fin_dat)
        yield company



