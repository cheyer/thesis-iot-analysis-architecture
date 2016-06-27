# Analytics
There are various ways to analyze data and gain insights, using different kinds of technologies and for different purposes.

1. Real Time Analytics (hot data)
2. Recent History Analytics (warm data)
3. Big Data and Analytics (cold data)

#### Real Time Analytics (hot data)
Analysis of incoming data in real time using rules to detect if the values surpass or fall behind a defined thresholds, which can trigger some actions. Patterns that are recognized in the long term analysis can be expressed in rules that can be used here.

#### Recent History Analytics (warm data)
Analysis of recent data, like the data of today / last week / last month. This is mostly being used for visualization and dashboard about the status quo. This can be realized by using e.g. the in-database analytics functionality of dashDB or connecting Cloudant to Spark to analyze the data in interactive notebooks.

#### Big Data and Analytics (cold data)
Analysis of a vast amount of data to gain new insights. Recognized patterns can be transformed into rules used in real time analytics. For this type of analytics other data sources like weather, social media data etc. are used to augment your own data and comprehend the correlations. There are two alternatives to do this:
Either you use Apache Hadoop (IBM BigInsights) and its Map Reduce paradigm or you use Apache Spark and its interactive Notebooks (Juypter Notebooks).

## Examples
Currently there are 3 analytical notebooks and one for loading data to the object storage:

1. Recent Cloudant: Load data from Cloudant and analyze it.
2. Recent Analytics: Load data from dashDB and analyze it.
3. History Analytics: Load data (in form of a parquet file) from the Object Storage and analyze it.
4. dashDB to Object Storage: Get data from dashDB and load it into the Object Storage as a parquet file.

## How to create the Spark Service and the Notebooks
If not done so before, deploy the Bluemix Services and Microservices via the shell script and verify that all services were created and are running:
```
./createServices.sh
./deployAllMicroservicesToBluemix.sh
```
2. Go to the Bluemix dashboard, scroll down to the list of your used services and click on the Spark service called **my-spark-service**.
3. Click on the button called "NOTEBOOKS" to launch the Spark Analytics dashboard.
4. Click on the tab "Object Storage", then click "Add Object Storage". To bind the existing Object Storage service click on the tab "Bluemix". Now you can select the Object Storage *my-objectstorage-service* and the container *my-container*. Confirm by clicking "SELECT".
5. Next, navigate to the "Data" tab (right-hand menu) and click on "Create A Connection". Create a connection to the previously created databases. Edit the name field, select *IBM Bluemix* for Service Type and select the Instance in the drop down menu. Do this for *my-dashDB-service* and *my-cloudant-db*!
6. Go to the "Analytics" tab (right-hand menu) and click on "New Notebook". Select the "From URL" tab in the head section and paste the GitHub URL of the notebook you want to create in the field "Notebook URL". If done so, submit by clicking "Create Notebook". Do this for *History Analytics.ipynb*, *Recent Analytics.ipynb*, *dashDB to Object Storage.ipynb* and *Recent Cloudant.ipynb*.
7. Back in the Analytics tab click on the notebook you've just created to open it.

## How to use
Before we can run the analytical notebooks, we have to copy data into the object storage. To do so, open the *dashDB to Object Storage.ipynb* notebook. In the menu, click on "Cell" and then "Run All". You should see the ouput by the running process directly printed in the notebook. Now you can run the other notebooks and modify them. Happy Analyzing!

## Alternatives
Instead of using the *dashDB to Object Storage* notebook for transferring the data you can set up a docker container running this job automatically. Therefore go to the folder **/docker data transfer**.
