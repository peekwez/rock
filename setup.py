from setuptools import find_packages, setup

setup(
    name='rock',
    version='0.0.1',
    url='https://github.com/peekwez/rock.git',
    license='Apache License, Version 2.0',
    author='Kwesi P. Apponsah',
    author_email='kwesi@kwap-consulting.com',
    packages=find_packages(exclude=['test','test.*']),
    entry_points={
        'console_scripts': [
            'rock.supervisor=rock.cmd_supervisor:main',
            'rock.service=rock.cmd_service:main',
            'rock.broker=rock.cmd_broker:main',
            'rock.syncdb=rock.cmd_syncdb:main'
        ]
    },
    install_requires=[
        "pyzmq==18.1.0",
        "msgpack==0.6.2",
        "pybranca==0.3.0",
        "Jinja2==2.10.1",
        "pyyaml==5.3.1",
        "boto3===1.12.32",
        "coloredlogs==14.0",
        "schemaless==0.0.1",
        "pymemcache==2.2.2",
        "redis==3.4.1"
    ],
    package_data={'rock': ['templates/*.txt']},
    extras_require={
        'dev': [
            'pytest',
            'flake8',
            'coverage'
        ],
    },
    description='ZeroMQ utility functions for services',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
