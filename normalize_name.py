import unicodedata
import re

def normalize_name(name):
    name = name.lower()
    name = strip_accents(name)
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
    text = text.replace("-", " ") # handle hyphenated names
    text = text.replace(".", "") # remove periods from "J.T. Brown" but we don't want to remove all punctuation e.g. "O'Connor"
    #text = re.sub(r'[^\w\s]', '', text) # remove punctuation (e.g. "J.T. Brown")
    return str(text)
