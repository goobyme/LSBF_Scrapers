# -*- coding: utf-8 -*-
import scrapy


class CarebCoreSpider(scrapy.Spider):
    name = 'careb_core'
    allowed_domains = ['caoreb.wildapricot.org']
    start_urls = ['https://caoreb.wildapricot.org/Sys/PublicProfile/8770609/3868214',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770575/3868214',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/30366988/3868214',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/30366992/3868214',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/30366802/3956418',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770682/3956418',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770660/3956418',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770628/3956391',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/30343894/3956391',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770716/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/30606837/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770708/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/30595712/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/30599932/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/30544469/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/9329889/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770744/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770698/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770737/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770689/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770719/3876400',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/30379432/3868229',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770495/3868229',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/8770493/3868229',
 'https://caoreb.wildapricot.org/Sys/PublicProfile/30379428/3868229']

    def parse(self, response):
        e = {'FirstName': response.css('span#FunctionalBlock1_ctl00_ctl00_memberProfile_MemberForm_memberFormRepeater_ctl00_TextBoxLabel2729469::text'
                                      ).extract_first(),
             'LastName': response.css('span#FunctionalBlock1_ctl00_ctl00_memberProfile_MemberForm_memberFormRepeater_ctl01_TextBoxLabel2729470::text'
                                     ).extract_first(),
             'Email': response.css('span#FunctionalBlock1_ctl00_ctl00_memberProfile_MemberForm_memberFormRepeater_ctl02_TextBoxLabel2729468 > a::text'
                                   ).extract_first(),
             'Phone':  response.css('span#FunctionalBlock1_ctl00_ctl00_memberProfile_MemberForm_memberFormRepeater_ctl03_TextBoxLabel2729473::text'
                                    ).extract_first(),
             'CAREBChapter': response.css('span#FunctionalBlock1_ctl00_ctl00_memberProfile_MemberForm_memberFormRepeater_ctl04_DropDownLabel7986464::text'
                                          ).extract_first(),
             'Company': response.css('div#idContainer2976744 div.fieldBody >span::text').extract_first(),
             'StreetAddress': response.css('div#idContainer2729478 div.fieldBody >span::text').extract_first(),
             'State': response.css('div#idContainer2729481 div.fieldBody >span::text').extract_first(),
             'City': response.css('div#idContainer2729479 div.fieldBody >span::text').extract_first(),
             'PostalCode': response.css('div#idContainer2729480 div.fieldBody >span::text').extract_first(),
             'Title': response.css('div#idContainer2729476 div.fieldBody >span::text').extract_first(),
             'Website': response.css('div#idContainer2729477 div.fieldBody a::text').extract_first(),
             'Link': response.url
             }
        yield e

