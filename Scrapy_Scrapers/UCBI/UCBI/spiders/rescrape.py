import scrapy
import re


class UCBIRescraperSpider(scrapy.Spider):
    name = "rescrape"
    allowed_domains = ["ucms.mortgage-application.net", "ucbi.com", "ucms.ucbi.com"]
    start_urls = ['https://ucms.mortgage-application.net/EmployeeList.aspx']

    def parse(self, response):
        self.log('Scraping {}...'.format(response.url))
        for link in response.css('a.Normal5Bold::attr(href)').extract():
            link = response.urljoin(link)
            yield scrapy.Request(url=link, callback=self.parse_elist)

    def parse_elist(self, response):
        for link in response.css('a.Normal5Bold::attr(href)').extract():
            link = response.urljoin(link)
            yield scrapy.Request(url=link, callback=self.parse_details)

    def parse_details(self, response):
        e = {
            'FullName': response.css('span.Header5::text').extract_first(),
            'StreetAddress': response.css('span#ctl01_contentContainerHolder_addressLabel::text').extract_first(),
            'Email': response.css('span#ctl01_contentContainerHolder_emailLabel > a::text').extract_first(),
            'Website': response.css('span#ctl01_contentContainerHolder_websiteLabel > a::text').extract_first(),
            'NMLS#': response.css('span#ctl01_contentContainerHolder_nmlsLoanOriginatorIDLabel::text').extract_first(),
            'DirectPhone': response.css('span#ctl01_contentContainerHolder_phoneLabel::text').extract_first()
        }
        phone_list = response.css('table#ctl01_contentContainerHolder_phonesTable > tr > td::text').extract()
        for i in range(int(len(phone_list)/2)):
            e[str(phone_list[i*2])] = phone_list[i*2 + 1].replace('\xa0', '')

        if e.setdefault('DirectPhone', None):
            e['DirectPhone'] = e.get('DirectPhone').replace('\xa0', '')

        cityregex = re.compile(r"^.+,")
        stateregex = re.compile(r"[A-Z]{2}")
        zipregex = re.compile(r"\d{5}$")
        loc_parse_line = response.css('span#ctl01_contentContainerHolder_stateLabel::text').extract_first()

        if cityregex.findall(loc_parse_line):
            e['City'] = cityregex.findall(loc_parse_line)[0]
        if stateregex.findall(loc_parse_line):
            e['State'] = stateregex.findall(loc_parse_line)[0]
        if zipregex.findall((loc_parse_line)):
            e['PostalCode'] = zipregex.findall(loc_parse_line)[0]

        if e.setdefault('Website', None):
            req = scrapy.Request(url=e['Website'], callback=self.deep_parse)
            req.meta['profile'] = e
            yield req
        else:
            e['Title'] = 'Mortgage Loan Originator'
            e['LocationsServed'] = ''
            yield e

    def deep_parse(self, response):
        e = response.meta['profile']
        try:
            e['Title'] = response.css('div.main-content > h4::text').extract_first().strip()
        except AttributeError:
            e['Title'] = 'Mortgage Loan Originator'
        e['LocationsServed'] = response.css('div#ctl00_Main_CMSPagePlaceholder2_lt_Main_UCBIContactDetails_pnl\RelatedLocations > ul > li > a::text').extract()
        yield e
