from setuptools import setup, find_packages

setup(name='piro',
      version="0.1.18",
      description='Piro is a tool for intelligently controlling services.',
      author='Paul Lathrop',
      author_email='paul@simplegeo.com',
      url='https://github.com/plathrop/piro',
      packages=find_packages(),
      install_requires=['simplegeo-penelope', 'thrift'],
      entry_points={'console_scripts':
                        ['piro = piro.cli:main']}
      )
