from argparse import ArgumentParser, Namespace
from typing import List, Tuple
from vizproc.bqtools import commit_tables

if __name__ == '__main__':

    parser: ArgumentParser = ArgumentParser(description='Script to manage BigQuery operations', epilog='by Xavi-Linux',
                                            add_help=True)
    parser.add_argument('-a', '--action', required=True, choices=['commit', 'rollback'], nargs=1,
                        help='Action to execute')
    parser.add_argument('tables', nargs='+', help='Tables to upload')
    parser.add_argument('-d', '--dataset', required=True, help='Dataset to create')
    parser.add_argument('-s', '--schema-path', required=True, help='Folder path containing schemas tables')
    parser.add_argument('-p', '--project-id', required=True, help='GCP project holding the dataset')
    parser.add_argument('-l', '--location', required=True, help='GCP location holding the dataset')

    args: Tuple[Namespace, List[str]] = parser.parse_known_args()
    if args[0].action[0] == 'commit':
        commit_tables(project_id=args[0].project_id, dataset=args[0].dataset,
                      location=args[0].location, schema_path=args[0].schema_path, tables=args[0].tables)
    else:
        print('To be implemented')
