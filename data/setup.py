from setuptools import find_packages, setup

setup(
    name="data",
    packages=find_packages(exclude=["data_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "Faker==18.4.0",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
