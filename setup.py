import io
import os
from setuptools import find_packages, setup


def read(*paths, **kwargs):
    content = ""
    with io.open(
            os.path.join(os.path.dirname(__file__), *paths),
            encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def read_requirements(path):
    return [
        line.strip()
        for line in read(path).split("\n")
        if not line.startswith(('"', "#", "-", "git+"))
    ]


setup(
        name="CartolaProject",
        version="0.1.0",
        description="Extract data football from api",
        url="CartolaData",
        python_requires=">=3.10",
        long_description_content_type="text/markdown",
        author="Gabriel Mizuno",
        packages=find_packages(exclude=["tests"]),
        include_package_data=True,
        install_requires=read_requirements("requirements.txt")
)