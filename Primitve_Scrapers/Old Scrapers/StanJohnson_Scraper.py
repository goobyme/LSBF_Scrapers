import requests
import bs4
import pandas
import re
import threading
import os
import vobject
import codecs
import logging
import shelve

THREADCOUNT = 5
SEARCHPAGE = 'http://www.kiddermathews.com/professionals_results.php?select_criteria=by_name&search=&type=search&search_submit=Search'
employees = []
init_proto_profile_list = []
teams = {}
elist_lock = threading.Lock()
llist_lock = threading.Lock()
team_lock = threading.Lock()


def webdl(url):
    """Downloads web-page, retries on initial failures, returns None if fails thrice (common!)"""
    print('Downloading...{}'.format(url))
    for i in range(3):
        try:
            r = requests.get(url)
            r.raise_for_status()
            return r
        except:     # Ugly but it works, hard to account for all the possible error codes otherwise
            print('[Warning webdl]: Retrying Download')
            continue
    print('[Error webdl]: Download failed for {}'.format(url))
    return None


def searchpageparsing(page):
    """Scrapes search page for individual parsing links to feed into threadbot system (not needed if pages # in url)"""
    if not page:    # Failed webdl handling
        return None
    proto_profiles = []

    soup = bs4.BeautifulSoup(page.text, 'lxml')
    parent_element = soup.find_all('dd', {'class': 'group'})

    for el in parent_element:
        e = {}
        link_el = el.find('a')
        if link_el:
            e['Link'] = link_el['href']
            e['Full Name'] = link_el.get_text()
        specialty_el = el.find('p', {'class': 'specialty'})
        if specialty_el:
            e['Specialty'] = specialty_el.get_text()
        proto_profiles.append(e)

    return proto_profiles


def employeelistparsing(page):
    """Parses each indv. profile listing page to return proto-profiles to be filled by visiting profile pages """
    if not page:    # Handling failed webdl
        return None
    proto_profiles = []
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    elements = soup.find_all('tr')
    for element in elements:
        e = {}
        link_parent = element.find('h3')
        if link_parent:
            link_el = link_parent.find('a')
        try:
            e['Link'] = link_el['href']
            e['Full Name'] = link_el.text
        except TypeError:
            continue
        department_el = element.find('td', {'width': '35%'})
        if department_el:
            e['Department'] = department_el.text.strip()

        proto_profiles.append(e)

    return proto_profiles


def vcfmuncher(link, thread_ident, name):
    """Downloads, saves, and edits (important!) VCF then feeds through parser to return employee profile json"""

    to_dl = webdl('http://www.kiddermathews.com' + link)
    if not to_dl:   # Handle webdl failure
        return None
    file_name = 'Kidder_{}_{}.vcf'.format(thread_ident, name)

    vcf_file = open(file_name, 'wb')
    for chunk in to_dl.iter_content(100000):
        vcf_file.write(chunk)
    vcf_file.close()
    print('Parsing VCF {}...'.format(file_name))
    try:    # Handle Unicode/read error
        parse_file = codecs.open(file_name, 'r', encoding='utf-8', errors='ignore')
        text_file = parse_file.read()
        parse_file.close()
    except UnicodeDecodeError:
        print('[Error-{} VCF Muncher]: File reading error'.format(thread_ident))
        return None

    """VCard Reformatter: Snips Vcard text so that Vobject can parse it wihtout errors"""
    photo_regex = re.compile(r"X-MS-CARDPICTURE.+(?=REV:)", flags=re.DOTALL)
    address_regex = re.compile(r"(?<=0A=)\s+(?=[A-Z]|[0-9])")
    line_regex = re.compile(r"-{10,}")
    label_regex = re.compile(r"LABEL.+")    # ANNOYING! Prevent Unicode error (removes instances of quoted printable)

    photo_remove = photo_regex.sub('\n', text_file)
    address_remove = address_regex.sub('', photo_remove, 3)
    line_remove = line_regex.sub('', address_remove)
    label_remove = label_regex.sub('', line_remove)
    e = vcfparsing(label_remove, thread_ident)
    return e


def vcfparsing(text, thread_ident):
    """Take vcard text contents, return JSON-structured employee. Try funcitons really are necessary :(
       Change specific attributes to match vcard formatting. To check attribute of vc object use debugger"""

    e = {}
    """Unicode or VObject Parsing handling"""
    try:
        vc = vobject.readOne(text)
    except UnicodeDecodeError:
        print('[Warning-{} VCF Parsing]: Default encoding error'.format(thread_ident))
        return None
    except vobject.base.ParseError:
        logging.exception('message')
        return None

    """Parsing portion"""
    e['Prefix'] = vc.n.value.prefix
    e['FirstName'] = vc.n.value.given
    e['LastName'] = vc.n.value.family
    e['MiddleName'] = vc.n.value.additional
    e['Suffix'] = vc.n.value.suffix
    try:
        if type(vc.adr.value.street) is str:
            adr_strip = vc.adr.value.street.replace('\r', '')
            e['Street'] = adr_strip.replace('\n', ', ')
        elif type(vc.adr.value.street) is list:
            e['Street'] = ', '.join([x.strip()
                                     for x in vc.adr.value.street])
    except AttributeError:
        pass
    try:
        e['City'] = vc.adr.value.city
    except AttributeError:
        pass
    except IndexError:
        pass
    try:
        e['State'] = vc.adr.value.region
    except AttributeError:
        pass
    except IndexError:
        pass
    try:
        e['PostalCode'] = vc.adr.value.code
    except AttributeError:
        pass
    except IndexError:
        pass
    try:
        e['Email'] = vc.email.value
    except AttributeError:
        pass
    try:
        e['Company'] = vc.org.value[0]
    except AttributeError or IndexError:
        pass
    try:
        e['Title'] = vc.title.value
    except AttributeError:
        pass
    try:
        tel_list = vc.tel_list
        for tel in tel_list:
            number = tel.value
            categories = ''
            for cat in tel.singletonparams:
                categories += cat
            e[categories] = number

    except AttributeError:
        pass

    return e


def propertyhunter(page):
    if not page:    # Handling failed webdl
        return None
    soup = bs4.BeautifulSoup(page.text, 'lxml')
    prop_el = soup.find_all('tr')
    types = []

    for el in prop_el:
        td_el = el.find_all('td')
        try:
            proptype = td_el[5]
            if proptype.text in types:
                continue
            else:
                types.append(proptype.text)
        except IndexError:
            continue

    output = ""
    for type in types:
        output += type + ', '

    return output


def personparsing(page, thread_ident, profile):
    """Parses from some combination of vcf and page and outputs json (to be iterated outside of func)"""

    try:    # Handle empty webdl failure
        soup = bs4.BeautifulSoup(page.text, 'lxml')
    except AttributeError:
        return profile
    e = profile

    # """VCF parsing subsection, kills early if vcf parse fails"""
    # vcfregex = re.compile(r"\.vcf")
    # vcf_parent = soup.find_all('a', {'class': 'link download'}, href=True)
    # for potential_link in vcf_parent:
    #     pot_link = potential_link['href']
    #     if vcfregex.findall(pot_link):
    #         e['VCard'] = pot_link.replace('.', '', 2)
    #     else:
    #         e['Bio'] = pot_link.replace('.', '', 2)
    # try:
    #     vcf_link = e['VCard']
    #     to_add = vcfmuncher(vcf_link, thread_ident, e['Full Name'])
    #     if not to_add:
    #         print('[Error-{} vcfmuncher]: VCF could not be downloaded/parsed'.format(thread_ident))
    #         return profile
    #     else:
    #         e.update(to_add)
    # except KeyError:
    #     print('[Error-{} personparser]: VCF element could not be located'.format(thread_ident))
    #     return profile

    """Page parsing subsection, expand/comment out as needed"""
    spec_parent = soup.find('div', {'class': 'thoroughfare'})
    try:
        e["Street"] = spec_parent.text.replace('\n', '')
    except AttributeError:
        pass
    spec_parent = soup.find('span', {'class': 'locality'})
    try:
        e["City"] = spec_parent.text
    except AttributeError:
        pass
    spec_parent = soup.find('span', {'class': 'country'})
    try:
        e["State"] = spec_parent.text
    except AttributeError:
        pass

    emailregex = re.compile(r'(?<=mailto:).+')
    linkregex = re.compile(r'www.linkedin.com')
    contact_parents = soup.find('div', {'class': 'kw-userOtherdetail'})
    link_els = contact_parents.find_all('a', href=True)
    for link in link_els:
        email = emailregex.findall(link['href'])
        linkedin = linkregex.findall(link['href'])
        if email:
            e['Email'] = link.text
        elif linkedin:
            e['LinkedIn'] = link['href']
        else:
            continue

    phoneregex = re.compile(r'(\d{3}[-.\s]??\d{3}[-.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-.\s]??\d{4}|\d{3}[-.\s]??\d{4})')
    phone_parent = contact_parents.find('div', attrs={'class': None})
    if phone_parent:
        phone_text = phone_parent.get_text()
        phone_hits = phoneregex.findall(phone_text)
        if phone_hits:
            e['Office Phone'] = phone_hits[0]
            try:
                e['Fax'] = phone_hits[1]
            except IndexError:
                pass

    eduregex = re.compile(r'University')
    try:
        bio_parent = soup.find('div', {'class': 'views-field views-field-field-education-professional-aff'})
        edu_parent = bio_parent.find('div', {'class':'field-content'})
        if edu_parent.find('p'):
            if eduregex.findall(edu_parent.get_text()):
                e['Education1'] = edu_parent.get_text()
        elif edu_parent.find('ul'):
            li_list = edu_parent.find_all('li')
            i = 1
            for li in li_list:
                if eduregex.findall(li.get_text()):
                    e['Education {}'.format(i)] = li.text
                    i += 1
    except AttributeError:
        pass

    team_parent = soup.find('div', {'class': 'col-lg-12 clearfix'})
    team_el = team_parent.find('a', {'class': 'kw-btnGreen'}, href=True)
    if team_el:
        linkregex = re.compile(r"(?<=team=).+")
        teamname = linkregex.findall(team_el['href'])
        if teamname:
            e['Team Name'] = teamname[0]

        team_lock.acquire()
        global teams
        try:
            if teamname[0] in list(teams.keys()):
                e['Property Type'] = teams[str(teamname[0])]
            else:
                prop_link = contact_parents.find('a', {'class': 'kw-btnGreen'}, href=True)
                if prop_link:
                    types = propertyhunter(webdl('https://www.stanjohnsonco.com' + prop_link['href']))
                    e['Property Type'] = types
                    teams[teamname[0]] = types
        finally:
            team_lock.release()

    return e


def threadbot(ident, total_len):
    """Reads global list_link for link to parse then parses to generate profile sublists , then merges with master"""

    print('Threadbot {} Initialized'.format(ident))
    sublist = []
    """Iterated function over global list with hand-coded queueing"""
    # TODO integrate built in Queue functionality into threadbot
    while True:
        llist_lock.acquire()
        if len(init_proto_profile_list) > 0:
            try:
                profile = init_proto_profile_list[0]    # Take proto-profile from list and removes from queue
                init_proto_profile_list.remove(profile)
                length = len(init_proto_profile_list)
            finally:
                llist_lock.release()
            print('Thread {} parsing link {} of {}'.format(ident, total_len - length, total_len))
            try:        # From here either feed through employeepageparsing or directly through personparsing
                link = profile['Link']
            except KeyError:
                print('[Error-{} Threadbot]: No link in proto-prof'.format(ident))
                continue
            full_prof = personparsing(webdl(link), ident, profile)
            sublist.append(full_prof)
        else:
            llist_lock.release()
            print('Thread {} completed parsing'.format(ident))
            break

    """Final merge with global list"""
    print('Thread {} writing to list...'.format(ident))
    elist_lock.acquire()
    try:
        global employees
        employees += sublist
    finally:
        elist_lock.release()


def main():
    os.chdir('/mnt/c/Users/Liberty SBF/Desktop/StanJohnson_VCF')
    # os.chdir('C:\\Users\\Liberty SBF\\Desktop\\StanJohnson_VCF')
    global employees
    global init_proto_profile_list
    shelf = shelve.open('StanJohnson_Links')
    init_proto_profile_list = shelf['Profiles']
    shelf.close()
    startlength = len(init_proto_profile_list)
    threads = []

    for i in range(THREADCOUNT):
        thread = threading.Thread(target=threadbot, args=(i+1, startlength, ))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    to_save = filter(None, employees)   # Gets rid of NoneTypes in list (very annoying if you don't do this!)
    data_frame = pandas.DataFrame.from_records(to_save)
    while True:
        try:
            data_frame.to_csv('StanJohnson.csv')
            break
        except PermissionError:
            print('Please close csv file')
            input()
    print('Done')


if __name__ == "__main__":
    main()

# TODO Improve means of variable manipulation so that blunt global variables + locking process is no longer needed
# TODO Add Time checking capabilities
# TODO Improve error handling
