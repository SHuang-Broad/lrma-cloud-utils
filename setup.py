from setuptools import setup

# Get the long description of the package:
with open("README.md", "r") as fh:
    long_description = fh.read()

# Get the license description:
with open("LICENSE.txt", "r") as fh:
    license_description = fh.readline()

with open('requirements.txt', 'r') as fh:
    to_be_installed = [l.rstrip('\n') for l in fh.readlines()]

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

    packages=['lrmaCU',
              'lrmaCU.cromwell',
              'lrmaCU.terra', 'lrmaCU.terra.submission', 'lrmaCU.terra.expt_design'],
    # packages=find_packages("src"),
    package_dir={'lrmaCU': 'src',
                 'lrmaCU.cromwell': 'src/cromwell',
                 'lrmaCU.terra': 'src/terra',
                 'lrmaCU.terra.submission': 'src/terra/submission',
                 'lrmaCU.terra.expt_design': 'src/terra/expt_design'},

    python_requires='>=3.7',
    install_requires=to_be_installed
)