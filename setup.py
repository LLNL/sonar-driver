from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
        name='sonar-create-ingestor',
        version='0.1',
        description='Scripts for creating Kafka connect ingestors on Sonar',
        long_description=readme,
        author='Alfredo Gimenez',
        author_email='gimenez1@llnl.gov',
        url='https://lc.llnl.gov/bitbucket/projects/SON/repos/create-ingestor',
        license=license,
        packages=find_packages(exclude=('tests', 'docs'))
        )
