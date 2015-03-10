try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from dnachat import VERSION


setup(name='dnachat',
      packages=['dnachat'],
      version=VERSION,
      description='Chatting server using bson on twisted with bynamodb & redis',
      author='Seungyeon Joshua Kim(Acuros)',
      author_email='acuroskr' '@' 'gmail.com',
      install_requires=[
          'bson>=0.3.3',
          'twisted',
          'redis',
          'boto',
          'bynamodb',
          'pytest'
      ],
      entry_points={
          'console_scripts': [
              'wsgit=wsgit.server:run'
          ]
      })
