import unicodedata
import re
from unidecode import unidecode

def normalize_name(name):
    name = name.lower()
    name = re.sub(r'\([^()]*\)', '', name)
    name = name.strip()
    name = unidecode(name) 
    name = name.replace('"', "") # remove quotes that will break the query
    name = name.replace("-", " ") # handle hyphenated names
    name = name.replace(".", "") # remove periods from "J.T. Brown" but we don't want to remove all punctuation e.g. "O'Connor"
    return name

print(normalize_name("Nicklas Nordgren (born 1979)"))