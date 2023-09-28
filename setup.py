from setuptools import setup, find_packages

with open('requirements.txt', 'r') as file:
    requirements = [line.strip() for line in file.readlines()]

setup(
    name="pnboia-glider-data-processor",
    version="0.1",
    description="",
    author="Thiago Caminha",
    author_email="thiago.caminha@marinha.mil.br",
    packages=find_packages(),
    install_requires=requirements,
)
