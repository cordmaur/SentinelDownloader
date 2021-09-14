from setuptools import find_packages, setup

setup(
    name='sentineldownloader',
    extras_require=dict(tests=['pytest']),
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        'pytest >= 6',
        'sentineldownloader >= 0.0.0',
        'matplotlib >= 3',
        'setuptools >= 52.0.0',
        'sentinelsat >= 1.1.0',
        'pandas >= 0.24',
        'ipywidgets',
        'jupyterlab_widgets',
    ]
)

