import requests, os, bs4


def webdl(url):

    r = requests.get(url)
    r.raise_for_status()

    html = bs4.BeautifulSoup(r.text)    # Flexible portion of code for how you want to parse the data
    return html

def htmlparsing(html):

    element = html.select() # Enter in what you wanna select



