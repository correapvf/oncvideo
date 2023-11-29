from setuptools import setup, find_packages

setup(
    name='oncvideo',
    version='1.0',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'oncvideo = oncvideo.arg_parser:main'
        ]
    }
)
