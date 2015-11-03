from setuptools import setup

setup(
    name = "json2run",
    version = "0.4",
    description = "A tool to launch experiments.",
    author="Tommaso Urli",
    author_email="tunnuz@gmail.com",
    license = "MIT",
    packages = ["json2run"],
    install_requires = [
        "pymongo",
        "scipy"
    ],
    scripts = ["./j2r"]
)
