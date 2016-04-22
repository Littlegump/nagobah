""" this script is design for dagobah"""

from distutils.core import setup
from setuptools import find_packages

setup(
        name = "nagobah",
        packages = find_packages(),
        version = "0.0.2",
        author = "xuanxuan",
        author_email = "13060404095@163.com",
        url = "https://github.com/littlegump/nagobah.git",
        classifiers = [
            "Programming Language :: Python",
            ],
        entry_points = {
            "console_scripts": [
                "nagobah = nagobah.nagobah:main",
                ],
            }
        )
