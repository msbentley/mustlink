from setuptools import setup

setup(name='mustlink',
    version='0.3',
    author='Mark S. Bentley',
    author_email='mark@lunartech.org',
    description='A python wrapper for the ESA mustlink API',
    long_description_content_type="text/markdown",
    url="https://github.com/msbentley/mustlink",
    download_url = 'https://github.com/msbentley/mustlink/archive/v0.3.tar.gz',
    python_requires='>=3.0',
    keywords = ['telemetry', 'MUST', 'ESA'],
    packages=['mustlink'],
    zip_safe=False)
