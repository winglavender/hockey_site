import unicodedata
import re
from unidecode import unidecode

def normalize_name(name):
    name = name.lower()
    name = re.sub(r'\([^()]*\)', '', name)
    name = name.strip()
    name = unidecode(name) # TODO test this
    # name_old = strip_accents(name)
    name = name.replace('"', "") # remove quotes that will break the query
    name = name.replace("-", " ") # handle hyphenated names
    name = name.replace(".", "") # remove periods from "J.T. Brown" but we don't want to remove all punctuation e.g. "O'Connor"
    return name


def strip_accents(text):
    try:
        text = unicode(text, 'utf-8')
    except NameError: # unicode is a default on python 3
        pass
    # handle Danish characters
    text = text.replace('å', 'a')
    text = text.replace('Å', 'A')
    text = text.replace('æ', 'ae')
    text = text.replace('Æ', 'AE')
    text = text.replace('ø', 'o')
    text = text.replace('Ø', 'O')
    text = unicodedata.normalize('NFD', text)\
           .encode('ascii', 'ignore')\
           .decode("utf-8")
    #text = re.sub(r'[^\w\s]', '', text) # remove punctuation (e.g. "J.T. Brown")
    return str(text)

print(normalize_name("Nicklas Nordgren (born 1979)"))