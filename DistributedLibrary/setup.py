from setuptools import setup, find_packages

setup(
    name="distributed-ml",
    version="0.2.6",
    author="Sumit Raut",
    author_email="sumitcraut@gmail.com",
    description="A package for distributed scikit-learn tasks.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/sumitraut7/CS230-distributed-machine-learning",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=["requests", "setuptools", "numpy", "pandas", "tqdm", "scikit-learn"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
