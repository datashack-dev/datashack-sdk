import os

import setuptools

version = "0.0.1"
setuptools.setup(
    name="datashack-sdk",
    version=version,
    author="Datashack",
    author_email="contact@datashack.dev",
    description="Datashack",
    packages=['datashack_sdk', 'datashack_sdk/*'],
    include_package_data=True,
    classifiers=[
    ],

    python_requires=">=3.8",
    install_requires=[
        # todo improve this package

    ],
    extras_require={
        
    },
)
