from setuptools import setup, find_packages

setup(
    name="distributed-ml",
    version="0.1.0",
    author="Your Name",
    author_email="sumitcraut@gmail.com",
    description="A package for distributed gridsearch over scikit models",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/sumitraut7/CS230-distributed-machine-learning",
    packages=find_packages(),
    install_requires=["requests"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
