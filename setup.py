from distutils.core import setup

setup(
    name="dazeus",
    version="0.1",
    author="Pol Van Aubel",
    author_email="dev@polvanaubel.com",
    url="https://github.com/MacGyverNL/dazeus-python3",
    description="Python 3 bindings for the DaZeus IRC bot",
    # long_description="TODO. But see the readme in the meantime",
    packages=["dazeus"],
    classifiers=["Programming Language :: Python",
                 "Programming Language :: Python :: 3",
                 #TODO select license.
                 "License :: Other/Proprietary License",
                 "Operating System :: OS Independent",
                 "Development Status :: 3 - Alpha",
                 "Intended Audience :: Developers",
                 "Environment :: Plugins",
                 "Topic :: Software Development :: Libraries",
                 "Topic :: Software Development :: Libraries :: "
                 "Python Modules",
                 "Topic :: Communications :: Chat :: Internet Relay Chat"]
)
