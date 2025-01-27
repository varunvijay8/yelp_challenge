from distutils.core import setup

def readme():
    """Import the README.md Markdown file and try to convert it to RST format."""
    try:
        import pypandoc
        return pypandoc.convert('README.md', 'rst')
    except(IOError, ImportError):
        with open('README.md') as readme_file:
            return readme_file.read()

setup(
    name='yelp_challenge',
    version='0.1',
    description='Analysis of the Yelp challenge dataset',
    long_description=readme(),
    classifiers=[
        'Programming Language :: Python :: 3',
    ],
    url='https://github.com/varunvijay8/yelp_challenge',
    author='Vijayakumaran Varun Vijay',
    author_email='varunvijay8@gmail.com',
    license='MIT',
    packages=[''],
)