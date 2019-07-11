# API downloader for Buildwith.com

This program built on [Databricks](https://en.wikipedia.org/wiki/Databricks) is a Python class that downloads data from Buildiwith.com via their [List API and Trend API](https://api.builtwith.com/). 

# API KEY

To run the program, first you would need to get API KEY from Buildwith.com and update the 'api_key' in the follow code:
```
def __init__(self, technologies, api='trend', api_key ='your API KEY'):
```
# Examples

For List API:

Steps:
	1. First, we need to create a list of technologies to download.
  ```
  technologies = ['Adobe PageMill', 'Adobe Portfolio',
                'Adobe Systems', 'Adobe Muse',
                'Adobe GoLive', 'Adobe Auditude',
                'Adobe RoboHelp', 'Adobe TagManager', 'Adobe Connect',
               ]
  ```
  
  2. Instantiate Downloader classa and start download the data.
  ```
  techs = BuiltwithDownloader(technologies, api='list')
  # download all data
  df = techs.download_list()
  ```
  
  # Additional feature
  
  You can also plot the data downloaded from the Trend API. But this feature only works on Databricks. 
  ```
  techs.plot_trend()
  ```
  
  
  
  
  
  


