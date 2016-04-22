""" this script is design for dagobah"""
# _*_ coding: utf-8 _*_

from distutils.core import setup
from setuptools import find_packages

setup(
        name = "nagobah",
        packages = find_packages(),
        version = "0.0.6",
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
