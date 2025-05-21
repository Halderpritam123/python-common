Uncovered code
from setuptools import find_packages, setup
setup(
    name="platform_common",
    version="2025.18.2",
    author="Albanero",
    description="Generic utility for Albanero platform services",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "boto3==1.34.16",
        "dataclass-wizard==0.22.2",
        "ec2-metadata==2.13.0",
        "flask==3.0.0",
        "gevent==23.9.1",
        "kafka-python==2.0.2",
        "pymongo==4.6.1",
        "regex==2023.6.3",
        "requests==2.32.3",
        "urllib3==1.26.18",
    ],
    python_requires=">=3.10",
)