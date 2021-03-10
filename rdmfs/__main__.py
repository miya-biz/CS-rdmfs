from argparse import ArgumentParser
import asyncio
import logging
import pyfuse3
import pyfuse3_asyncio
from rdmfs import fs
from osfclient import cli


pyfuse3_asyncio.enable()

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')
    parser.add_argument('-u', '--username', default=None,
                        help=('OSF username. Provide your password via '
                              'OSF_PASSWORD environment variable'))
    parser.add_argument('--base-url', default=None,
                        help='OSF API URL (Default is https://api.osf.io/v2/)')
    parser.add_argument('-p', '--project', default=None,
                        help='OSF project ID')
    return parser.parse_args()


def main():
    options = parse_args()
    init_logging(options.debug)

    osf = cli._setup_osf(options)
    rdmfs = fs.RDMFileSystem(osf, options.project)
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=rdmfs_asyncio')
    if options.debug_fuse:
        fuse_options.add('debug')
    pyfuse3.init(rdmfs, options.mountpoint, fuse_options)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(pyfuse3.main())
    except:
        pyfuse3.close(unmount=False)
        raise
    finally:
        loop.close()

    pyfuse3.close()


if __name__ == '__main__':
    main()
