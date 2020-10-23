import tkinter as tk
from tkinter import messagebox, filedialog
import snowflake.connector
import pandas as pd
import os

class Snow():
    def __init__(self, root):
        self.root = root
        self.root.title("Intructions and Process [Important]*")
        self.root.geometry(
            "400x500+100+100")  
        frame1 = tk.Frame(self.root)
        frame1.place(x=300, y=200, width=350, height=350)

        point_1_label = tk.Label(self.root, text="1. Login into Snowflake*", padx=10, pady=10)
        point_2_label = tk.Label(self.root, text="2. create a stage*", padx=10, pady=10)
        point_3_label = tk.Label(self.root, text="3. Input Schema and Database for Snowflake", padx=10, pady=10)
        nextb = tk.Button(self.root, text="Next", command=lambda: Snow.snowflake_connection_schema(self))

        point_1_label.place(x=50, y=100)
        point_2_label.place(x=50, y=170)
        point_3_label.place(x=50, y=240)
        nextb.place(x=150, y=310, width=100)

    def snowflake_connection_schema(self):
        global snowflake_login
        global username_snow
        global password_snow
        global account_snow
        self.root.destroy()
        self.root = tk.Tk()
        snowflake_connection(self.root)


class snowflake_connection():

    def __init__(self, root):
        self.root = root
        self.root.title("Snowflake Login")
        self.root.geometry(
            "265x350+100+100")  

        frame1 = tk.Frame(self.root)
        frame1.place(x=400, y=300, width=350, height=350)

        username_snow_label = tk.Label(self.root, text="Username*")
        password_snow_label = tk.Label(self.root, text="Password*")
        account_snow_label = tk.Label(self.root, text="Account*")
        db_snow_label = tk.Label(self.root, text="Database*")
        schema_snow_label = tk.Label(self.root, text="Schema*")

        nextb = tk.Button(self.root, text="Next", command=lambda: snowflake_connection.stage(self))
        snow_login_button = tk.Button(self.root,
                                      command=lambda: snowflake_connection.snowflake_connection_verification(self),
                                      text="Login")
        self.username_snow = tk.StringVar()
        self.password_snow = tk.StringVar()
        self.account_snow = tk.StringVar()
        self.db_snow = tk.StringVar()
        self.schema_snow = tk.StringVar()


        # entry widget
        username_snow_entry = tk.Entry(self.root, textvariable=self.username_snow)
        password_snow_entry = tk.Entry(self.root, textvariable=self.password_snow, show='*')
        account_snow_entry = tk.Entry(self.root, textvariable=self.account_snow)
        db_snow_entry = tk.Entry(self.root, textvariable=self.db_snow)
        schema_snow_entry = tk.Entry(self.root, textvariable=self.schema_snow)


        username_snow_label.place(x=30, y=30)
        password_snow_label.place(x=30, y=70)
        account_snow_label.place(x=30, y=110)
        db_snow_label.place(x=30,y=150)
        schema_snow_label.place(x=30,y=190)


        username_snow_entry.place(x=100, y=30)
        password_snow_entry.place(x=100, y=70)
        account_snow_entry.place(x=100, y=110)
        db_snow_entry.place(x=100, y=150)
        schema_snow_entry.place(x=100,y=190)

        snow_login_button.place(x=80, y=250)
        nextb.place(x=150, y=250)

    def stage(self):

        self.root.destroy()
        self.root = tk.Tk()
        newstage(self.root)
        
    def create_stage(self):
        
        global table
        table = self.stage_name.get()
        sql = f"create or replace stage {table} file_format = (type ='csv' field_delimiter =',')"
        cursor_snow.execute(sql)
        listcreated = cursor_snow.execute("LIST @"+table)
        if len(listcreated.fetchall()) >=0:
            msgBox=messagebox.askquestion('Stage Creation', "Stage Created Succesfully, Do you want to load the data to stage?")
            if msgBox == 'yes':
                nextb = tk.Button(self.root, text="Upload Files", command=lambda:snowflake_connection.UploadActions(self))
                nextb.place(x=150, y=120)
            else:
                messagebox.showinfo('Thanks', "Exiting the application")
                self.root.destroy()
        else:
            messagebox.showinfo('Stage Creation', "Stage Creation Failed!")
        
    def UploadActions(self):
        li = []
        self.filename = filedialog.askopenfilenames()
        for i in range(0,len(self.filename)):
            print(self.filename[i])
            file = pd.read_csv(self.filename[i],index_col=None,header=0)
            li.append(file)
        final = pd.concat(li,axis=0,ignore_index=True)
        upload_file = "uploaded_file.csv"
        final.to_csv(upload_file,index=False,header=False)
        print(os.getcwd().replace('\\','/'))
        put_script = 'put file:///' + os.getcwd().replace('\\','/') + "/" + upload_file + ' @' + self.stage_name.get()
        a = cursor_snow.execute(put_script)
        if len(a.fetchall()) >=0:
            flashlbl= tk.Label(self.root,text="Files Loaded to stage successfully",fg="green")
            flashlbl.place(x=20,y=170)
            msgBox=messagebox.askquestion('Stage Creation', "Files copied to stage successfully, Do you want to load the data to tables?")
            if msgBox == 'yes':
                flashlbl.destroy()
                self.table_snow = tk.StringVar()
                tablelabel = tk.Label(self.root,text="Select Table")
                AA = cursor_snow.execute('SHOW TABLES')
                S = AA.fetchall()
                tablelist = pd.DataFrame(S).loc[:,1:1]
                tablelist.columns=['TABLES']
                self.uniquetablelist = tablelist['TABLES'].unique().tolist()
                print(self.uniquetablelist)
                self.table_snow.set(self.uniquetablelist[0])
                table_snow_entry = tk.OptionMenu(self.root, self.table_snow, *self.uniquetablelist)
                tablelabel.place(x=20,y=200)
                table_snow_entry.place(x=100,y=195)
                btnshow = tk.Button(self.root,text='update',command=self.select)
                btnshow.place(x=190,y=195)
                
                
            else:
                messagebox.showinfo('Thanks', "Exiting the application")
                self.root.destroy()
    

    def snowflake_connection_verification(self):
        # global variable
        global con_snow
        global cursor_snow
        # global connection_snow
        # importing values
        username_sf = self.username_snow.get()
        password_sf = self.password_snow.get()
        account_sf = self.account_snow.get()
        db_sf = self.db_snow.get()
        schema_sf = self.schema_snow.get()
    
        con_snow = snowflake.connector.connect(user=username_sf, password=password_sf, account=account_sf)
        cursor_snow = con_snow.cursor()
        connection_snow = True
    
        if connection_snow == True:
            cursor_snow.execute("CREATE DATABASE IF NOT EXISTS " + db_sf)
            cursor_snow.execute("USE " + db_sf)
            cursor_snow.execute('CREATE SCHEMA IF NOT EXISTS ' + schema_sf)
            cursor_snow.execute("USE SCHEMA " + schema_sf)
            con_snow = snowflake.connector.connect(user=username_sf, password=password_sf, account=account_sf, database=db_sf,schema=schema_sf)
            cursor_snow = con_snow.cursor()
            messagebox.showinfo('Snowflake Login', "Connection Successful!")
            
        else:
            messagebox.showerror('Snowflake Login', 'Connection Error!')


class newstage():

    def __init__(self, root):
        self.root = root
        self.root.title("Stage Creation")
        self.root.geometry("275x270+100+100")

        stagename_snow_label = tk.Label(self.root, text="Stage Name")

        snow_login_button = tk.Button(self.root,
                                      command=lambda: snowflake_connection.create_stage(self),
                                      text="Create")
        home_button = tk.Button(self.root, command=lambda: newstage.home(self), text="Login")
        
        self.stage_name = tk.StringVar()

        stage_name_entry = tk.Entry(self.root, textvariable=self.stage_name)
        stagename_snow_label.place(x=20, y=70)
        stage_name_entry.place(x=100, y=70)
        home_button.place(x=200, y=10)
        snow_login_button.place(x=100, y=120)

    def select(self):
        global tabsel
        
        tabsel = self.table_snow.get()
        copy_script = 'copy into ' + tabsel + ' from @' + self.stage_name.get()
        print(copy_script)
        
        AAB = cursor_snow.execute('SELECT COUNT(*) FROM '+tabsel)
        countbef = int(str(AAB.fetchall())[2:-3])
        
        try:
            cpy = cursor_snow.execute(copy_script)
        except:
            messagebox.showerror("Failed","Failed to load to table, Check if you have table present\
            and data matches correctly between csv file and tables. see the history for more details.")
            exit_button = tk.Button(self.root, command=lambda: self.root.destroy(), text="Exit")
            exit_button.place(x=210, y=230)
        print(cpy.fetchall())
        print(tabsel)
        AAA = cursor_snow.execute('SELECT COUNT(*) FROM '+tabsel)
        countaft = int(str(AAA.fetchall())[2:-3])
        if countaft > countbef:
            flashlbl2= tk.Label(self.root,text="Files Loaded to Table successfully",fg="green")
            flashlbl2.place(x=20,y=230)
            exit_button = tk.Button(self.root, command=lambda: self.root.destroy(), text="Exit")
            exit_button.place(x=210, y=230)
        else:
            flashlbl3= tk.Label(self.root,text="Files Loaded to Table Failed, See History for more details",fg="red")
            flashlbl3.place(x=20,y=230)
            exit_button = tk.Button(self.root, command=lambda: self.root.destroy(), text="Exit")
            exit_button.place(x=210, y=230)
            
            
    def home(self):
        self.root.destroy()
        self.root = tk.Tk()
        snowflake_connection(self.root)


if __name__ == '__main__':
    root = tk.Tk()
    ob = Snow(root)
    root.mainloop()
