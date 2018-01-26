import re

page = 'https://commercial.century21.com/search.c21?lid=SAK&t=2&o=&s=0&subView=searchView.Paginate'

pagenumberregex = re.compile(r"(?<=&s=)\d+")
pagenumberregex.sub(str(int(pagenumberregex.findall(page)[0]) + 10), page)

print(page)


