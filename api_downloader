# Load packages
import pyspark.sql.functions as F
import pandas as pd
import datetime as Dt
import urllib
import json
print('importing Euphrates Downloader...')

# plotting
import matplotlib as mpl
import seaborn as sns; sns.set_style("whitegrid")
import matplotlib.pyplot as plt
import matplotlib as mpl
# Update matplotlib defaults to MScience style 
mpl_update = {'figure.figsize':[7.5,5.0],
              'axes.color_cycle':["#005a69", "#f7941e", "#5cb6c0", "#acd466", "#cf0072",
                                  "#337B87", "#F9A94B", "#7DC5CD", "#BDDD85", "#D9338E",
                                  "#5C959F", "#FABA6F", "#ACDADF", "#D5E9B1", "#E77DB7"]
              }

mpl.rcParams.update(mpl_update)

class EuphratesDownloader:
  
    def __init__(self, technologies, api='trend'):
        dbutils.widgets.removeAll()
        
        if api == 'trend':
            dbutils.widgets.text('since', '2011-01-01','Since')
            dbutils.widgets.dropdown('plot', technologies[0], [tech for tech in technologies], 'Plot')
          
        if api == 'list':
            dbutils.widgets.text('firstbetween', '2010-01-01 and today','FIRSTBETWEEN')
            dbutils.widgets.text('lastbetween', '2010-01-01 and today','LASTBETWEEN')
        
        self.techs = technologies

    def download_trend(self):
        # Set up Parameters to pass to the API (API address and keys)
        url_key ='https://api.builtwith.com/trends/v6/api.json?KEY=1d0ed17a-8530-447b-86e5-ccde3b8294f1'
        # Create an empty list to save fetched data
        data_list = [[]]

        # Fetch all data
        for tech in self.techs:
          
            print(tech)
            tech = '&TECH=' + tech.replace(' ', '-')
            # Set date range, default to monthly dates unless the parameter - freq is modified
            start_date = dbutils.widgets.get('since')
            end_date = Dt.datetime.today().date().strftime('%Y-%m-%d')
            date_range = pd.date_range(start= start_date, end = end_date, freq='M')

            for i in date_range:
                date = i.strftime('%Y-%m-%d')
                # Add date to url
                url = url_key + tech + '&DATE='+date
                # Connect to the API and load data
                try:
                  response = urllib.urlopen(url)
                  raw_data = json.loads(response.read())
                except ValueError:
                  print('HTTP status error code: {}'.format(response.getcode()))
                # Get data for variables
                live = raw_data.get('Tech').get('Coverage').get('Live')
                top_1m = raw_data.get('Tech').get('Coverage').get('Mil')
                top_1hk = raw_data.get('Tech').get('Coverage').get('HundredK')
                top_1tk = raw_data.get('Tech').get('Coverage').get('TenK')
                name = raw_data.get('Tech').get('Name')
                # Put data into a list and append all the list 
                data_list.append([name, date, live, top_1m, top_1hk, top_1tk])

        # Convert all the lists into a dataframe, drop the first row since it is the empty list we created earlier.
        df = pd.DataFrame(data_list,columns = ['name', 'date', '1. All Internet', '2. Top 1M', '3. Top 100K', '4. Top 10K'])[1:]
        df = df.drop_duplicates()
        self.df = df
        # Download data 
        df_melt = pd.melt(df, id_vars = ['name', 'date'], value_vars = [col for col in df.columns[2:]], var_name = 'rank', value_name = 'count')
        final_data = (   
                   spark.createDataFrame(df_melt)
                   .withColumn('date', F.to_date(F.col('date')))
        )
        return final_data

    def plot_trend(self):
        # Make quick plots 
        try:
            df = self.df
        except:
            print('The data may not be in the memory, please try rerunning the script.')

        choose = df[df.columns[0]] == dbutils.widgets.get('plot')
        plot = df[choose]
        plot['date'] = pd.to_datetime(plot['date'])

        fig, (ax1, ax2, ax3, ax4) = plt.subplots(nrows = 1, ncols = 4, 
                                                 figsize = (20, 5))

        ax1.plot(plot['date'], plot[df.columns[2]])
        ax2.plot(plot['date'], plot[df.columns[3]])
        ax3.plot(plot['date'], plot[df.columns[4]])
        ax4.plot(plot['date'], plot[df.columns[5]])

        ax1.set_title(df.columns[2])
        ax2.set_title(df.columns[3])
        ax3.set_title(df.columns[4])
        ax4.set_title(df.columns[5])

        ax1.set_ylim(ymin=0)
        ax2.set_ylim(ymin=0)
        ax3.set_ylim(ymin=0)
        ax4.set_ylim(ymin=0)

        display(plt.show())
    
    def download_list(self):
        # Set up Parameters to pass to the API (API address and keys)
        url_key ='https://api.builtwith.com/lists2/api.json?KEY=1d0ed17a-8530-447b-86e5-ccde3b8294f1'
        firstbetween = '&FIRSTBETWEEN=' + dbutils.widgets.get('firstbetween')
        lastbetween = '&LASTBETWEEN=' + dbutils.widgets.get('lastbetween')
        # Create an empty list to save fetched data
        data_list = []
        # Fetch all data
        for tech in self.techs:
            print(tech)
            tech2 = '&TECH=' + tech.replace(' ', '-')
            url = url_key + tech2 + lastbetween + firstbetween
            # Connect to the api
            try:
              response = urllib.urlopen(url)
              raw_data = json.loads(response.read())
            except ValueError:
              print('HTTP status error code: {}'.format(response.getcode()))
            # Initiate data retrieving  
            data = pd.DataFrame(raw_data.get('Results'))
            page_read = 1
            next_page = str(raw_data.get('NextOffset'))
            # This if statement handles errors when there is no data available for a particular technology.
            if data.shape[0] == 0:
                print(str(raw_data.get('Errors')))
                continue
            else:
                # Read through each page and fetch the data
                while next_page != 'END':
 
                    url_next_page = url+ '&OFFSET=' + next_page
                    response = urllib.urlopen(url_next_page)
                    raw_data = json.loads(response.read())
                    data = data.append(pd.DataFrame(raw_data.get('Results')))
                    next_page = str(raw_data.get('NextOffset'))
                    page_read += 1

                print('rows x columns: ' + str(data.shape), 'page read: ' + str(page_read))
            
            
                # Change column names
                new_cols = ['Alexa_Rank', 'Country', 'Domain', 'First_Detected', 'First_Indexed', 'Last_Detected', 'Last_Indexed', 'Quantcast_Rank']
                data.columns = new_cols
                try:
                    # Convert Epoch seconds to date
                    for col in data[['First_Detected', 'First_Indexed', 'Last_Detected', 'Last_Indexed']].columns:
                        data[col] = pd.to_datetime(data[col], unit='s')
                        
                except Exception as e: 
                    print(e)
                    continue
                # Add an addtional column to identify the technology
                data['technology'] = tech
                data_list.append(data)
        # Now we are out of all the iterations
        # We can combine the data 
        all_data = pd.concat(data_list)    
        final_data = spark.createDataFrame(all_data)
        return final_data
