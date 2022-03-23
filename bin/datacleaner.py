from vizproc import exceltocsv
from argparse import ArgumentParser, Namespace
from typing import Tuple, List, Union

if __name__ == '__main__':
    parser: ArgumentParser = ArgumentParser(description='It converts every Excel worksheet into an independent '
                                                        'csv file.', epilog='by Xavi-Linux', add_help=True)
    parser.add_argument('-f','--file', help='File to clean',
                        required=True)
    parser.add_argument('-d', '--dest', help='Folder to store the cleaned datasets',
                        required=True)
    parser.add_argument('-p', '--pipeline', help='The argument must match the NAME property of a Pipeline class '
                                                 'defined in pipelines.py. If not found, it defaults to BasePipeline.',
                        default='base')

    parser.add_argument('-s', '--schema_folder', help='Folder to store schemas of tables')

    args:Tuple[Namespace, List[str]] = parser.parse_known_args()

    output: Union[str, List[str]] = exceltocsv(**vars(args[0]))
    if isinstance(output, str):
        print(output)
    else:
        print(' '.join(output))

