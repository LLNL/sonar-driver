from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='sonar-driver',
    version='0.1',
    description='Scripts for driving Sonar',
    long_description=readme,
    author='Alfredo Gimenez',
    author_email='gimenez1@llnl.gov',
    url='https://lc.llnl.gov/bitbucket/projects/SON/repos/sonar-driver',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    include_package_data=True,
    install_requires=[
        'avro-python3',
        'cassandra-driver',
        'click',
        'requests',
        'pygments',
        'ipython',
        'findspark',
        'pyspark',
        'pandas<0.21',
        'plotly',
        'bokeh',
        'seaborn',
        'ipywidgets',
        'sh',
        'nltk'
    ],
    entry_points='''
        [console_scripts]
        sonar-driver=sonar_driver.cli.cli:cli
    ''',
)
