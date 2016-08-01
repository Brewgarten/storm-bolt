import sys

from setuptools import setup, find_packages

import versioneer


needs_pytest = {"pytest", "test", "ptr", "coverage"}.intersection(sys.argv)
pytest_runner = ["pytest-runner"] if needs_pytest else []

setup(
    author = "IBM",
    author_email = "",
    cmdclass=versioneer.get_cmdclass(),
    description = "Cloud configuration and setup",
    entry_points = {
        "console_scripts" : [
            "storm-bolt = storm.bolt.manager:main",
        ]
    },
    install_requires = [
        "apache-libcloud==1.1",
        "c4-utils",
        "prettytable==0.7.2",
        "storm-thunder"
    ],
    keywords = "python storm cloud setup",
    license = "IBM",
    name = "storm-bolt",
    packages = find_packages(),
    setup_requires=[] + pytest_runner,
    tests_require=["pytest", "pytest-cov"],
    url = "",
    version = versioneer.get_version(),
)
