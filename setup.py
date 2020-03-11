from setuptools import setup

setup(
    name='rock',
    version='0.0.1',
    url='https://github.com/peekwez/rock.git',
    license='Apache License, Version 2.0',
    author='Kwesi P. Apponsah',
    author_email='kwesi@kwap-consulting.com',
    packages=[
        'rock',
    ],
    entry_points={
        'console_scripts': ['rock=rock.command_line:main']
    },
    install_requires=[
        "pyzmq==18.1.0",
        "msgpack==0.6.2",
        "pybranca==0.3.0"
    ],
    include_package_data=True,
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
