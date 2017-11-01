import bs4
import urllib


BK_PAGE = "https://www.berkadia.com/people-and-locations/people/"



with urllib.request.urlopen(BK_PAGE) as page:
    soup = bs4.BeautifulSoup(page.content(), 'lxml').find_all('div', {'class': 'col-xs-9 col-sm-9 col-md-10'})
    for link in [x.a['href'] for x in soup.find_all('h2')]:
        print(link)



