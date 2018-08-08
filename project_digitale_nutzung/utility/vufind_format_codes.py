import requests


def swissbib_format_codes() -> dict:
    file = requests.get('https://raw.githubusercontent.com/swissbib/vufind/master/local/languages/formats/de.ini')

    result = dict()
    text = file.text.split("\n")
    for line in text:
        if line.startswith(';') or line == '':
            pass
        else:
            temp = line.split('=')
            code = temp[0].strip()
            if '/' in code:
                code = code[1:]
                code = code.replace('/', '')
            value = temp[1].strip(' ').strip('"')
            result[code] = value
    return result
