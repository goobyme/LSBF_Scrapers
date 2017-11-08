
def personparsing(page):       #TODO test out bs4 selectors and then ur golden!

    es = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    table = soup.find_all('tbody')
    elements = table[0].find_all('tr')

    for element in elements:
        e = {}

        name = element.find_all('a')[0]
        e["Name"] = name['title']

        email = element.find_all('a')[1]
        em = email.get_text()
        e["Email"] = em

        loc_parent = element.find_all('td')
        loc = loc_parent[len(loc_parent) - 1]
        e["Location"] = loc.get_text

        title = element.find_all('td')
        e['Title'] = title[1]

        phone = element.find_all('td')
        e["Phone"] = phone[2].get_text

        specs = element.find_all('small')
        if specs:
            e["Specialties"] = specs[0].get_text

        es.append(e)

    return es


