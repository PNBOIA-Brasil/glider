from setuptools import setup, find_packages

with open("README.md", "r", encoding = "utf-8") as fh:
    long_description = fh.read()

with open('requirements.txt', 'r') as file:
    requirements = [line.strip() for line in file.readlines()]

setup(
    name = "pnboia-glider",
    version = "0.0.1",
    author = "Thiago Caminha",
    author_email = "thiago.caminha@marinha.mil.br",
    description = "package for processing glider data",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "package URL",
    project_urls = {
        "Bug Tracker": "package issues URL",
    },
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir = {"": "src"},
    packages = find_packages(where="src"),
    python_requires = ">=3.6",
    install_requires=requirements,
    scripts=['scripts/pnboia-glider-decoder',
                'scripts/glider-etl']
)
