import os
import setuptools

version = "0.0.16"

# read the contents of your README file
with open('./README.md') as f:
    long_description = f.read()


setuptools.setup(
    name="datashack_sdk",
    version=version,
    author="Datashack",
    author_email="contact@datashack.dev",
    # other arguments omitted
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=["datashack", "datashack_sdk_py"],
    include_package_data=True,
    classifiers=[
    ],
    entry_points = {
        'console_scripts': ['datashack=datashack.cli:main']
    },
    python_requires=">=3.8",
    install_requires=[
        # todo improve this package
        "kinesis-python",
        "jsonschema==4.17.3",
        "moto==4.1.0",
        "dacite",
        "click",
        "rich",
        "inquirer",
        "pyyaml",
        "dataclasses",
        "SQLAlchemy",
        "fastapi",
        "cdktf",
        "cdktf_cdktf_provider_aws",
        "cryptography==38.0.4", # https://stackoverflow.com/questions/74981558/error-updating-python3-pip-attributeerror-module-lib-has-no-attribute-openss
    ],
    extras_require={
        "tests": [
            "pytest-runner",
            "pytest",
            "requests-mock",
            "mock",
            "moto==4.1.0",
            "boto3==1.26.52",
            "freezegun",
            "aioresponses"
        ],
    },
)
