import rock as rk

FILES = (
    ("Make", "in"),
    ("Makefile", ""),
    ("MANIFEST", "in"),
    ("run", "sh"),
    ("setup", ".py"),
    ("docker", ".run")
)
PROJECT = ("__init__.py", "service.py", "exceptions.py")


def create_files(rootdir, service):
    import os
    from jinja2 import Environment, FileSystemLoader

    _one = f'{rootdir}/{service}'
    _two = f'{rootdir}/{service}/{service}'
    _env = rk.utils.loader('rock', 'templates')
    context = {'service': service}

    # create level 1 files
    if not os.path.exists(_one):
        os.makedirs(_one)
    for filename, ext in FILES:
        temp = f'{filename}.txt'
        rendered = rk.utils.render(_env, temp, context)
        filepath = f'{_one}/{filename}.{ext}'
        with open(filepath, 'w') as f:
            f.write(rendered)

    # create level 3 files
    if not os.path.exists(_two):
        os.makedirs(_two)
    for filename in PROJECT:
        filepath = f'{_two}/{filename}'
        with open(f'{filepath}', 'w') as f:
            pass


def main():

    from argparse import ArgumentParser
    parser = ArgumentParser()

    parser.add_argument(
        '-d', '--rootdir', dest='rootdir',
        help='project root directory'
    )
    parser.add_argument(
        '-s', '--service', dest='service',
        help='microservice name',
    )

    options = parser.parse_args()
    create_files(options.rootdir, options.service)


if __name__ == "__main__":
    main()
