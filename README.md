# Tkinter_Snowflake-load
Easy Tkinter app to login to snowflake, create stage, load data to stage, load from stage to table

This is a GUI built with Python Tkinter library, where we have specified different classes with few functions. Clone and run the python script. 

First screen shows the instructions page. Upon clicking next, you will be asked to enter the login details for snowflake. Once entered, click on Login button to verify if the connection is succesful. If the connection is succesful, window message appears to inform whether the login was success or not. 

Next, click on Next will open a new frame which asks us to enter the stage name that is already existing or create a new one. It checks and creates if the stage is not present. Press create once the stage name is entered. Then another pop up asking yes or no for uploading files. 

Once we press "yes", a new button "upload files" will appear, clicking on it will open file dialog where we can select multiple csv files that has the same structure of the table we want to load for. 

Once the files has been uploaded, we will get a message and then asks whether you want to upload to tables or not, if selected "yes", then new buttons comes to frame, one is the drop down list to ask us to select the table, once selected relevant table, click on update. If the update has been proper, we will get a message saying that table has been uploaded successfully and a exit button that stops the process and exit. If failed to upload, then get a pop up message




