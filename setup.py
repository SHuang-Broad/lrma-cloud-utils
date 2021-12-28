from setuptools import find_packages, setup

# Get the long description of the package:
with open("README.md", "r") as fh:
    long_description = fh.read()

# Get the license description:
with open("LICENSE.txt", "r") as fh:
    license_description = fh.readline()

setup(
    name="lrmaCU",
    version="0.0.1",
    author="Steve Huang",
    author_email="shuang@broadinstitute.org",
    description="A (experimental) package supporting LRMA's routine cloud operations.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    url="https://github.com/SHuang-Broad/lrma-cloud-utils",
    license=license_description,
    project_urls={
        'Source': 'https://github.com/SHuang-Broad/lrma-cloud-utils',
    },

    packages=['lrmaCU', 'lrmaCU.cromwell', 'lrmaCU.terra'],
    # packages=find_packages("src"),
    package_dir={'lrmaCU': 'src',
                 'lrmaCU.cromwell': 'src/cromwell',
                 'lrmaCU.terra': 'src/terra'},

    install_requires=[
        'firecloud',
        'google-cloud-storage',
        'jupyter',
        'numpy',
        'pandas',
        'pandas-selectable',
        'python-dateutil',
        'requests',
        'termcolor'
    ],
    python_requires='>=3.7'
)