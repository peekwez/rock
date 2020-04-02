import rock as rk

ROOT = (
    "Make",
    "Makefile",
    "MANIFEST",
    "run",
    "setup",
    "docker"
)

EXTS = (
    ".in",
    "",
    ".in",
    ".sh",
    ".py",
    ".run"
)


PROJECT = (
    "__init__.py",
    "service.py",
    "exceptions.py"
)


def create_files(rootdir, service):
    import os
    from jinja2 import Environment, FileSystemLoader

    _one = f'{rootdir}/{service}'
    _two = f'{rootdir}/{service}/{service}'
    samples = f'{rootdir}/sample'
    _env = rk.utils.loader('rock', 'templates')
    # Environment(
    #    loader=FileSystemLoader(samples)
    # )
    context = {'service': service}

    # create level 1 files
    if not os.path.exists(_one):
        os.makedirs(_one)
        for k, file in enumerate(ROOT):
            filename = f'{file}.txt'
            rendered = rk.utils.render(_env, filename, context)
            newfile = f'{_one}/{file}{EXTS[k]}'
            with open(newfile, 'w') as f:
                f.write(rendered)

    # create level 3 files
    if not os.path.exists(_two):
        os.makedirs(_two)
        for file in PROJECT:
            filename = f'{_two}/{file}'
            with open(filename, 'w') as f:
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
