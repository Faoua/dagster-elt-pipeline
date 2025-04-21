from setuptools import find_packages, setup

setup(
    name="proj_data_final",
    packages=find_packages(exclude=["proj_data_final_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
