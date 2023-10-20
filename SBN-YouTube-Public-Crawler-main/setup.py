from setuptools import setup, find_packages

with open('requirements.txt', 'r', encoding='utf-8') as f:
    install_req = f.read().splitlines()

setup(
    name='sbn_yt_pu_crawler',
    version='1.0.1',
    url='',
    license='MIT',
    author='Sandbox Network Inc.',
    author_email='dev@sandboxnetwork.net',
    description='YouTube Public Data Crawler',
    packages=find_packages(),
    long_description=open('README.md', 'r', encoding='utf-8').read(),
    install_requires=install_req,
    zip_safe=False,
    setup_requires=[]
)