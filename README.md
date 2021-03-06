# SentinelDownloader
The <b>SentinelDownloader</b> package provides a very simple Python interface to download Sentinel imagery from the <b>Copernicus Open Access Hub</b> (https://scihub.copernicus.eu/). It should be mentioned though, that most heavy lifting tasks such are provided by the <b>SentinelSat</b> api (https://github.com/sentinelsat/sentinelsat), but the <b>SentinelDownloader</b> exposes some fancy functionalities in a convenient objected oriented interface. <br>
<br>
Simply put, SentinelDownloader is a high level object oriented wrapper to the SentinelSat api, that wraps the Copernicus api. Something like the image bellow: 
![image](https://user-images.githubusercontent.com/19617404/132748919-e8d1f0dd-083e-4a39-86f8-eeadfb84a4e9.png)

The <b>SentinelDownloader</b> provides:
* Object oriented interface
* Asynchronous downloading
* Quickview grid to easily identify the images to download
* Simpler searching methods

![image](https://user-images.githubusercontent.com/19617404/132750257-6c5e906f-80b2-492e-b6c6-d2be6d48d5df.png)


## Instalation
To install sentineldownloader it is necessary to clone the repository and install it from the root trough the following commands:
```
git clone https://github.com/cordmaur/SentinelDownloader.git
cd SentinelDownloader

pip install -r requirements.txt

pip install -e .
```

# Manual
The manual is located in the `/nbs` folder.
https://github.com/cordmaur/SentinelDownloader/blob/main/nbs/01_Manual.ipynb


