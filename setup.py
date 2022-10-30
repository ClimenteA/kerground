from setuptools import setup, find_packages

# python3 setup.py bdist_wheel sdist
# cd dist
# twine upload *


with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="kerground",
    version="0.1.1",
    description="Stupid simple background worker based on python.",
    url="https://github.com/ClimenteA/kerground",
    author="Climente Alin",
    author_email="climente.alin@gmail.com",
    license="MIT",
    py_modules=["kerground"],
    install_requires=[],
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    # entry_points={"console_scripts": ["kerground=kerground:cli"]},
)
