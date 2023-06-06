# Phonepe-pulse
Hi !!!! This is the app I created to analyse Phonepe pulse data ... Data cloned  from the official phonepe gitup repository and dne all analysation and extratraction process ...Then I deployed this app in streamlit.Enjoy the code 😉![world-map-line-business-connection-free-video](https://github.com/Rabbit3112/Phonepe-Data-visualization/assets/121392940/ce816a20-c9b1-4860-9e0b-27da09c27855)

First lets extracts datas from the github and convert it into csv files
This is the dataset i used in my streamlit web application to visualize datas..

After cloning files from github repo i created a for loop to loop through each folder and get datas from it and then append it to a dataframe to make it easy to covert to csv.

![code](https://github.com/Rabbit3112/Phonepe-Data-visualization/assets/121392940/d2003529-574f-4371-8eae-854f76ddb14b)

Now we want to repeat this process for all respective folders then we can get all the data in our desired format of csv.

After extracting the data we need to upload it into Mysql
To insert datas into Mysql i used Mysql connector python(you can use sqlalchemy also)


![visualization](https://github.com/Rabbit3112/Phonepe-Data-visualization/assets/121392940/291f88de-c134-42fe-975b-6a8552ee3732)



In order to insert csv to Mysql we need to establish connection to Mysql server with local host.

Then after inserting all my files to Mysql database. I created a new file named main.py to create a app using streamlit.

![app preview](https://github.com/Rabbit3112/Phonepe-Data-visualization/assets/121392940/580e121a-26c4-42c8-9ab5-035209ace568)

Enjoy coding !!!!!!😉😉😉😉😉😉😉😉😉😉😉😉😉😉😉😉
