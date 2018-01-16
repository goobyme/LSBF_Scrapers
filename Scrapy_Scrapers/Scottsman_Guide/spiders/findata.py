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
        ref = {'Commercial': '49', 'Hard Money': '10', 'Multifamily': '50', 'Construction': '52',
               'Nonprime Niches': '10', 'Branch Opportunities': '18', 'Wharehouse Lending': '12',
               'Correspondent Lending': '19', 'Prime Niches': '4', 'Education': '23'}
        url_list = []
        for item in matrix_id:
            parse_id = ref.setdefault(item, None)
            if parse_id:
                url_list.append("""http://www.scotsmanguide.com/rsPopDefault.aspx?ucAdd=1001&MTXID={}&LenderId={}""".format(
                    parse_id, lend_id))
            else:
                continue

        company['Finance URL'] = url_list
        yield company
        # if url_list:
        #     req = scrapy.Request(url=url_list[0], callback=self.parse_finance)
        #     req.meta['company'] = company
        #     yield req
        # else:
        #     yield company

    def parse_finance(self, response):
        self.log('Scraping{}'.format(response.url))
        fin_dat = {}
        descriptors = []
        e_list = response.css('td.LSRbody1CwtextC')
        for el in e_list:
            check = el.css('abbr::text').extract_first()
            if check == 'Y':
                descriptors.append(el.css('abbr::attr(title)').extract_first())
            elif check:
                fin_dat[el.css('abbr::attr(title)').extract_first()] = check
            else:
                continue
        fin_dat.update({
            'Firm Type': response.css('td[colspan="4"]::text').extract_first(),
            "Specialties/Loan Type": descriptors
        })
        company = response.meta['company']
        company.update(fin_dat)
        yield company



