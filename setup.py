from setuptools import setup, find_packages

# python setup.py bdist_wheel sdist
# cd dist
# twine upload *


with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="kerground",
    version="0.0.1",
    description="Simple background worker based on pickle and sqlite",
    url="https://github.com/ClimenteA/kerground",
    author="Climente Alin",
    author_email="climente.alin@gmail.com",
    license="MIT",
    py_modules=["kerground"],
    install_requires=["python-dotenv"],
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    entry_points={"console_scripts": ["kerground=kerground:cli"]},
)