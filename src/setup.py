import setuptools

setuptools.setup(
    name="neurocaas_cli", # Replace with your own username
    version="0.0.1",
    author="Taiga Abe",
    author_email="ta2507@columbia.com",
    description="CLI usage for NeuroCAAS.",
    long_description="Package and repository for users to interact with NeuroCAAS via CLI.",
    long_description_content_type="text/markdown",
    url="https://github.com/cunningham-lab/neurocaas_cli",
    packages=setuptools.find_packages(),
    install_requires=[
        "Click",
    ],
    entry_points={
        "console_scripts":[
            "neurocaas-cli = neurocaas_cli.main:main",
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.6.6',
    include_package_data=True,
    package_data={"":["*Dockerfile","*.json","*.txt"]}
)

