from selenium import webdriver
from pprint import pprint

link_list = ['https://caoreb.wildapricot.org/Central-Valley',
 'https://caoreb.wildapricot.org/inland-empire',
 'https://caoreb.wildapricot.org/San-Diego',
 'https://caoreb.wildapricot.org/San-Diego-Photos',
 'https://caoreb.wildapricot.org/Los-Angeles',
 'https://caoreb.wildapricot.org/northlosangeles',
 'https://caoreb.wildapricot.org/Oakland',
 'https://caoreb.wildapricot.org/Orange-County',
 'https://caoreb.wildapricot.org/page-1479602',
 'https://caoreb.wildapricot.org/page-1479592',
 'https://caoreb.wildapricot.org/Solano',
 'https://caoreb.wildapricot.org/Solano-Photos']
ProfileLinks = list()
browser = webdriver.Firefox()

for link in link_list:
    browser.get(link)
    profile_els = browser.find_elements_by_css_selector('h5 > a')
    for el in profile_els:
        profile_link = el.get_attribute('href')
        ProfileLinks.append(profile_link)

pprint(ProfileLinks)

