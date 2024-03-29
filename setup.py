from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='mustlink',
    version='0.3.2',
    author='Mark S. Bentley',
    author_email='mark@lunartech.org',
    description='A python wrapper for the ESA mustlink API',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/msbentley/mustlink",
    download_url = 'https://github.com/msbentley/mustlink/archive/0.3.2.tar.gz',
    install_requires=['matplotlib','pandas','pyyaml','requests','pysocks'],
    python_requires='>=3.0',
    keywords = ['telemetry', 'MUST', 'ESA'],
    zip_safe=False)
