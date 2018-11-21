from setuptools import setup
setup(
    name="examples",
    version='0.1.0',
    namespace_packages=['serialization'],
    packages=['serialization.avro'],
    install_requires=[
        'confluent-kafka[avro]',
        'six'
    ],
    entry_points={
        'console_scripts': [
            'avro-cli = serialization.avro.__main__:main'
        ]
    })
