from typing import Union, Iterable
from requests import get, Response
from bs4 import BeautifulSoup, element
from pathlib import Path
from io import StringIO
import re

__URL: str = 'https://www.workout-wednesday.com/power-bi-challenges/'
__TARGET_CLASS: str = 'elementor-post__thumbnail__link'
__TARGET_FOLDER: str = './'
__TARGET_REQUIREMENTS_WRAPPER: str = 'elementor-text-editor elementor-clearfix'
__README: str = 'README.md'


def url_to_folder_name(url: str) -> Union[str, None]:
    url_slice: str = url[-9:-5] + 'W' + url[-3:-1]
    if re.match('20[0-9]{2}W[0-9]{2}', url_slice):

        return url_slice

    return None


def convert_to_markdown_list(html: element.Tag, left_spaces: int=0) -> str:
    tags: Iterable[element.Tag] = filter(lambda t: isinstance(t, element.Tag), html)
    markdown_str: str = ''
    for tag in tags:
        tag: element.Tag
        if tag.name == 'li':
            if len(tag.findChildren('li')) == 0:
                markdown_str += ' ' * (2**left_spaces - 1) + '- ' + tag.text + '\n'

            else:
                max_pos: int = tag.text.find(':') if tag.text.find(':') > tag.text.find('.') else tag.text.find('.')
                markdown_str += ' ' * (2**left_spaces - 1) + '- ' + tag.text[:max_pos+1] + \
                                '\n' + convert_to_markdown_list(tag.findChildren('li'), left_spaces + 1) + '\n'

    return markdown_str


def scrap_challenge(url: str) -> str:
    request: Response = get(url)
    parsed_text: BeautifulSoup = BeautifulSoup(request.text, 'html.parser')

    header: str = "Source: [Link]({0})\n\n###Requirements\n\n".format(url)
    requirements: element.ResultSet = parsed_text.find_all(attrs={'class': __TARGET_REQUIREMENTS_WRAPPER})
    tags: Iterable[element.Tag] = filter(lambda t: isinstance(t, element.Tag), requirements[1].children)
    for tag in tags:
        tag: element.Tag
        if tag.name == 'p':
            header += tag.text + '\n\n'

        elif tag.name == 'ul':
            header += convert_to_markdown_list(tag.children, 0)

    return header


def scrap_challenges(url: str=__URL, target_path: str=__TARGET_FOLDER) -> None:
    request: Response = get(url)
    main_page: str = request.text

    parsed_text: BeautifulSoup = BeautifulSoup(main_page, 'html.parser')
    links: element.ResultSet = parsed_text.find_all('a', attrs={'class': __TARGET_CLASS})
    for tag in links:
        tag: element.Tag
        folder: Union[str, None] = url_to_folder_name(tag['href'])
        if isinstance(folder, str):
            dest_path: Path = Path(target_path + folder)
            if not dest_path.exists():
                dest_path.mkdir()
                text: str = scrap_challenge(tag['href'])
                with open(str(dest_path) + '/' + __README, 'w') as f:
                    f: StringIO
                    f.write(text)

            else:

                return None

