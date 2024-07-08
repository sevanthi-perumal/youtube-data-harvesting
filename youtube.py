
from googleapiclient.discovery  import build
import pandas as pd
import pymongo
import mysql.connector
import streamlit as st
import seaborn as sns



def API_Connect():
    api_id = "AIzaSyA3qABo-55Xw93nw_A5kuqMyO_8qFHctLg"
    api_server_name = "youtube"
    api_version = "v3"
    youtube =build(api_server_name,api_version,developerKey=api_id)
    return youtube

youtube =API_Connect()



def get_channel_stats(channel_ids):
    all_data = []
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_ids
                )
            
    response=request.execute()

 
    for i in range(0,len(response["items"])):
        all_data = dict(Channel_Name = response["items"][i]["snippet"]["title"],
                    Channel_Id = response["items"][i]["id"],
                    Subscription_Count= response["items"][i]["statistics"]["subscriberCount"],
                    Views = response["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response["items"][i]["snippet"]["description"],
                    Playlist_Id = response["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"]
                    )
       
        
    
    return all_data

# get_channel_stats("UCyLbyQ4hUElQqJOJXFL485A")

# get playlist details
         

def get_playlist_details(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data


# get_video_ids("UCyLbyQ4hUElQqJOJXFL485A")


def get_video_ids(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


def get_video_details(video_ids):
    all_video_stats = []
    for video_id in video_ids:
        request = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id= video_id)
        response = request.execute()
   
        
        for video in response['items']:
            video_stats = dict(channel_id =video['snippet']['channelId'],
                             video_id =video["id"],
                             video_name = video['snippet']['title'],
                             video_description =video['snippet']['description'],
                             video_thumbnails =video['snippet']['thumbnails']["default"]["url"],
                             video_tag = ",".join(video['snippet'].get('tags',[])),
                             Published_date = video['snippet']['publishedAt'],
                             views_count = video['statistics'].get('viewCount'),
                             Likes_count = video['statistics']['likeCount'],
                             Favorite_Count =video['statistics']['favoriteCount'],
                             Comments_count = video['statistics'].get('commentCount'),
                             Duration =video["contentDetails"]["duration"],
                             Caption_Status = video['contentDetails']['caption']
                        
                              )
            all_video_stats.append(video_stats)
    
    return all_video_stats



#  get comment details

def get_comment_details(video_ids):
    all_comment_stats =[]
    try:
        for i in video_ids:
            request = youtube.commentThreads().list(
                part ='snippet',
                videoId =i,
                maxResults =50
                            )
            response = request.execute() 
        
        
        
            for comment in response['items']:
                    comment = dict(Video_Id = comment['snippet']['videoId'],
                            Comment_Id = comment['snippet']['topLevelComment']['id'],
                            Comment_Text = comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_PublishedAt = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                    ) 
                    
                    all_comment_stats.append(comment)
                    
            
    except:
        pass        
            
    return all_comment_stats   
 


# comment_details = get_comment_details(video_ids) 

                

        
#  create the cilent for mongadb  

client =pymongo.MongoClient("mongodb+srv://sripathi:sripathi@cluster0.kk3lgof.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_Details"]

#  load the data to mongadb server
def Channel_Details(channel_ids):
    Channel_Details =get_channel_stats(channel_ids)
    Playlist_Details =get_playlist_details(channel_ids)
    Video_Id =get_video_ids(channel_ids)
    Video_Details =get_video_details(Video_Id)
    Comment_Details =  get_comment_details(Video_Id)

    coll1 = db["Channel_Details"]
    coll1.insert_one({"Channel_Information" :Channel_Details,"Playlist_Information" : Playlist_Details,'Video_Information' : Video_Details,"Comment_Information":Comment_Details})

    
    return " UPLOADED COMPLETED SUCCESSFULLY"


# create table


"channel"
def  channels_table():
        mysql1 = mysql.connector.connect(
                host="localhost",
                user="root",
                password="sripathi@2002",
                database="youtube_data",
                port="3306"
                    )
        cursor = mysql1.cursor() 

    


        drop_query ='''drop table if exists channels'''
        cursor.execute(drop_query)
        mysql1.commit()

        
        try :
            qurey = """create table if not exists channels(channel_name VARCHAR(100),
                                            channel_id VARCHAR(100) PRIMARY KEY,
                                            subscribers INT,
                                            view INT,
                                            total_videos INT,
                                            channel_description TEXT,
                                            playlist_id VARCHAR(100)
                )"""


            cursor.execute(qurey)
            mysql1.commit()
            
        except:
            print(" TABLE IS ALREADY CREATED")
            
            
            
        ch_list =[]
        db =client["Youtube_Details"]
        colli = db["Channel_Details"] 
        for ch_data in colli.find({},{'_id':0,'Channel_Information':1}):
            ch_list.append(ch_data["Channel_Information"])
        df = pd.DataFrame(ch_list)  



        for index,row in df.iterrows():
            insert_query =''' insert into channels(channel_name,
                                                channel_id,
                                                subscribers,
                                                view,
                                                total_videos,
                                                channel_description,
                                                playlist_id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
                                                
                                                
            value=(row["Channel_Name"],
                row["Channel_Id"],
                row["Subscription_Count"],
                row["Views"],
                row["Total_Videos"],
                row["Channel_Description"],
                row["Playlist_Id"])  
            
            try:
                cursor.execute(insert_query,value) 
                mysql1.commit()
                
            except:
                print("channel infrmation is aready inserted")     
     
  
#  create playkist table 
            
def playlist_table():           
    mysql1 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="sripathi@2002",
    database="youtube_data",
    port="3306"
    )
    cursor = mysql1.cursor()         

    drop_query ='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mysql1.commit()

    try :
        qurey = """create table playlists(PlaylistId VARCHAR(100) PRIMARY KEY,
                                        Title  VARCHAR(100) ,
                                        ChannelId VARCHAR(100),
                                        ChannelName VARCHAR(100),
                                        PublishedAt TIMESTAMP,
                                        VideoCount INT
                                              )"""


        cursor.execute(qurey)
        mysql1.commit()
        
    except:
        print(" TABLE IS ALREADY CREATED")           
                
    
    ch_list =[]          
    db =client["Youtube_Details"]
    coll1 = db["Channel_Details"] 
    for ch_data in coll1.find({},{'_id':0,'Playlist_Information':1}):
        for i in range(len(ch_data["Playlist_Information"])):  
            ch_list.append(ch_data["Playlist_Information"][i])
    df = pd.DataFrame(ch_list)       


    for index,row in df.iterrows():
        row["PublishedAt"]=pd.to_datetime(row["PublishedAt"])
        insert_query ='''insert into playlists(PlaylistId,
                                                Title,
                                                ChannelId,
                                                ChannelName,
                                                PublishedAt,
                                                VideoCount)
                                            
                                            value(%s,%s,%s,%s,%s,%s)'''
                                            
                        
                                            
        value=(row["PlaylistId"],
            row["Title"],
            row["ChannelId"],
            row["ChannelName"],
            row["PublishedAt"],
            row["VideoCount"]
            )  
        
        try:
            cursor.execute(insert_query,value) 
            mysql1.commit()
            
        except:
            print("channel infrmation is aready inserted")  
 
    
    
 
 
def video_table():    
    
    mysql1 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="sripathi@2002",
    database="youtube_data",
    port="3306"
    )
    cursor = mysql1.cursor()        

    drop_query ='''drop table if exists videos'''
    cursor.execute(drop_query)
    mysql1.commit()


    try :
        query = """create table if not exists videos(channel_id VARCHAR(100)  ,
                             video_id VARCHAR(100) PRIMARY KEY ,
                             video_name VARCHAR(100),
                             video_description TEXT,
                             video_thumbnails VARCHAR(100),
                             video_tag TEXT,
                             Published_date DATE,
                             views_count BIGINT,
                             Likes_count BIGINT,
                             Favorite_Count INT,
                             Comments_count INT,
                             Duration TIME,
                             Caption_Status VARCHAR(100)
                                   
            )"""


        cursor.execute(query)
        mysql1.commit()
        
    except:
        print(" TABLE IS ALREADY CREATED")           
                
    ch_list =[]          
    db =client["Youtube_Details"]
    coll1 = db["Channel_Details"] 
    for ch_data in coll1.find({},{'_id':0,'Video_Information':1}):
        for i in range(len(ch_data["Video_Information"])):  
            ch_list.append(ch_data["Video_Information"][i])
    df = pd.DataFrame(ch_list)  

    for index,row in df.iterrows():
               
        row["Published_date"]=pd.to_datetime(row["Published_date"])
         
        duration_seconds = pd.to_timedelta(row["Duration"]).seconds
        row["Duration"] = f"{duration_seconds // 3600:02d}:{(duration_seconds % 3600) // 60:02d}:{duration_seconds % 60:02d}"
        
         

        
        insert_query ='''insert into videos(channel_id,
                                               video_id,
                                                video_name,
                                                video_description,
                                                video_thumbnails,
                                                video_tag,
                                                Published_date,
                                                views_count,
                                                Likes_count,
                                                Favorite_Count,
                                                Comments_count,
                                                Duration, 
                                                Caption_Status 
                                                )values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
          
        value= (row["channel_id"],
                row["video_id"],                                 
                row["video_name"],
                row["video_description"],
                row["video_thumbnails"],
                row["video_tag"],
                row["Published_date"],
                row["views_count"],
                row["Likes_count"],
                row["Favorite_Count"],
                row["Comments_count"],
                row["Duration"], 
                row["Caption_Status"]
                )  
 
        try:
                cursor.execute(insert_query,value) 
                mysql1.commit()
                
        except:
                print("channel infrmation is aready inserted")   
    
    
 


# create comment table
 
 
def comment_table():           
    mysql1 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="sripathi@2002",
    database="youtube_data",
    port="3306"
    )
    cursor = mysql1.cursor() 
    
    drop_query ='''drop table if exists comments'''
    cursor.execute(drop_query)
    mysql1.commit()

    try :
        qurey = """create table comments(Video_Id VARCHAR(100),
                           Comment_Id VARCHAR(100) PRIMARY KEY,
                            Comment_Text TEXT,
                            Comment_Author VARCHAR(100),
                            Comment_PublishedAt DATE
                            )"""


        cursor.execute(qurey)
        mysql.commit()
        
    except:
        print(" TABLE IS ALREADY CREATED")           
                
    
    ch_list =[]          
    db =client["Youtube_Details"]
    coll1 = db["Channel_Details"] 
    for ch_data in coll1.find({},{'_id':0,'Comment_Information':1}):
        for i in range(len(ch_data["Comment_Information"])):  
            ch_list.append(ch_data["Comment_Information"][i])
    df= pd.DataFrame(ch_list)  

    for index,row in df.iterrows():
         
        row["Comment_PublishedAt"]=pd.to_datetime(row["Comment_PublishedAt"])
        insert_query ='''insert into comments(Video_Id,
                                            Comment_Id,          
                                            Comment_Text ,
                                            Comment_Author ,
                                            Comment_PublishedAt)
                                                    
                                            values(%s,%s,%s,%s,%s)'''
                                            
                    
                                            
        value=(row["Video_Id"],
            row["Comment_Id"],
            row["Comment_Text"],
            row["Comment_Author"],
            row["Comment_PublishedAt"]
              )  
        
        try:
            cursor.execute(insert_query,value) 
            mysql1.commit()
            
        except:
            print("channel infrmation is aready inserted")   

        
    

def create_tables():
     
        channels_table()
        playlist_table()
        video_table()
        comment_table()                           
                            
        return "Table Create Successfully" 
 




def create_channel_table():
    ch_list =[]
    db =client["Youtube_Details"]
    coll1 = db["Channel_Details"]    
    for ch_data in coll1.find({},{'_id':0,'Channel_Information':1}):
        ch_list.append(ch_data["Channel_Information"])
    df = st.dataframe(ch_list)
        
    return df   
        
            
            
   
def create_playlist_table():
    ch_list =[]          
    db =client["Youtube_Details"]
    coll1 = db["Channel_Details"] 
    for ch_data in coll1.find({},{'_id':0,'Playlist_Information':1}):
        for i in range(len(ch_data["Playlist_Information"])):  
            ch_list.append(ch_data["Playlist_Information"][i])
    df = st.dataframe(ch_list)       

    return df   
            
           
           
def create_video_table():             
    ch_list =[]          
    db =client["Youtube_Details"]
    coll1 = db["Channel_Details"] 
    for ch_data in coll1.find({},{'_id':0,'Video_Information':1}):
        for i in range(len(ch_data["Video_Information"])):  
            ch_list.append(ch_data["Video_Information"][i])
    df = st.dataframe(ch_list)     
    
    return df   
    
    
def create_comment_table():
    ch_list =[]          
    db =client["Youtube_Details"]
    coll1 = db["Channel_Details"] 
    for ch_data in coll1.find({},{'_id':0,'Comment_Information':1}):
        for i in range(len(ch_data["Comment_Information"])):  
            ch_list.append(ch_data["Comment_Information"][i])
    df = st.dataframe(ch_list)    
    
    return df   





with st.sidebar:
    st.title(":red[YOUTUBE DATA HARCESTING AND WAREHOUSING]")
    st.header("skills developed")
    st.caption("python scripting")
    st.caption("API integration")
    st.caption("data management in mongodb and mysql")
    
channel_id =st.text_input("ENTER THE CHANNEL ID")
    
if st.button("COLLECTE AND STORE DATA"):
    ch_list =[]
    db = client["Youtube_Details"]
    coll1 =db["Channel_Details"]
    for ch_data in coll1.find({},{'_id':0,'Channel_Information':1}):
        ch_list.append(ch_data["Channel_Information"]["Channel_Id"])
        
    if channel_id in ch_list:
        print(st.success("THE CHANNEL ID IS ALREADY INSERTED"))
    else:
        insert =Channel_Details(channel_id)  
        print(st.success("INSERT"))
    
    
if st.button("MIGRATE TO SQL"):
    Table =create_tables()
    st.success(Table)
    
    
selecte_table=st.radio("SELECTE THE TABLE TO VIEW",("CHANNEL","PLAYLISTS","VIDIEOS","COMMENTS"))

if selecte_table == "CHANNEL":
    create_channel_table()
    
elif selecte_table == "PLAYLISTS":
    create_playlist_table()
    
elif selecte_table == "VIDIEOS":
    create_video_table()
    
elif selecte_table =="COMMENTS":
    create_comment_table()
    
    
mysql1 = mysql.connector.connect(
                host="localhost",
                user="root",
                password="sripathi@2002",
                database="youtube_data",
                port="3306"
                    )
cursor = mysql1.cursor() 

questions = st.selectbox("SELECT YOUR QUESTIONS",("1. the names of all the videos and their corresponding channels",
                                                  "2. channels have the most number of videos, and how many videos do they have",
                                                  "3. the top 10 most viewed videos and their respective channels",
                                                  "4. comments were made on each video, and what are their corresponding video names",
                                                  "5. videos have the highest number of likes, and what are their corresponding channel names",
                                                  "6. the total number of likes and  for each video, and what are their corresponding video names",
                                                  "7. the total number of views for each channel, and what are their corresponding channel names",
                                                  "8. the names of all the channels that have published videos in the year 2022",
                                                  "9. the average duration of all videos in each channel, and what are their corresponding channel names",
                                                  "10. videos have the highest number of comments, and what are their  corresponding channel names"
                                                 ))


mysql1 =mysql.connector.connect(
    host ="localhost",
    user ="root",
    password = "sripathi@2002",
    database="youtube_data",
    port = "3306"
     )
cursor = mysql1.cursor()

if questions =="1. the names of all the videos and their correspon…":
    qurey ='''select c.channel_name as "Channel Name" , v.video_name as "Video Name"
              from channels c 
              join videos v on c.channel_id = v.channel_id'''
              
    cursor.execute(qurey)
    t1 =cursor.fetchall()
    df =pd.DataFrame(t1,columns =["Channel Name","Video Name"])
    st.write(df)

elif questions == "2. channels have the most number of videos, and how many videos do they have":
    query ='''SELECT c.channel_name as "Channel Name", c.total_videos "Total Number Of Video"
              FROM channels c
              order by c.total_videos desc'''

    
    cursor.execute(query)
    t1 =cursor.fetchall()
    df =pd.DataFrame(t1,columns =["Channel Name","Total Number Of Video"])
    st.write(df)
    
elif questions ==  "3. the top 10 most viewed videos and their respective channels":
    query ='''SELECT c.channel_name as "Channel Name" ,v.video_name as "Video Name",v.views_count as "Number Of Views"
              from videos v
              join channels c on v.channel_id = c.channel_id
              order by v.views_count desc
              limit 10'''
            
    cursor.execute(query)
    t1 =cursor.fetchall()
    df =pd.DataFrame(t1,columns =["Channel Name","Video Name","Number Of Views"])
    st.write(df)        
            
    
    
elif questions ==  "4. comments were made on each video, and what are their corresponding video names":
    query ='''select v.video_name as"Video Name" , v.Comments_count as "Number Of Comments"
              from videos v'''
    
    cursor.execute(query)
    t1 =cursor.fetchall()
    df =pd.DataFrame(t1,columns =["Video Name","Number Of Comments"])
    st.write(df) 


elif questions == "5. videos have the highest number of likes, and what are their corresponding channel names":

    query ='''SELECT c.channel_name as "Channel Name",v.video_name as "video Name",v.Likes_count as "Number Of Likes"
                FROM channels c 
                join videos v on v.channel_id = c.channel_id
                order by v.Likes_count desc'''    
                    
    cursor.execute(query)
    t1 =cursor.fetchall()
    df =pd.DataFrame(t1,columns =["Channel Name","video Name","Number Of Likes"])
    st.write(df) 
    
        

elif questions == "6. the total number of likes and  for each video, and what are their corresponding video names":
    query = '''SELECT v.video_name as "Video Name",v.Likes_count as "Number Of Likes"
               from videos v'''
    
    cursor.execute(query)
    t1 =cursor.fetchall()
    df =pd.DataFrame(t1,columns =["Video Name","Number Of Likes"])
    st.write(df)
    
    
    
elif questions == "7. the total number of views for each channel, and what are their corresponding channel names":
    query ='''SELECT c.channel_name "Channel Name",c.view as "Total Number OF View"
              from channels c'''
              
       
    cursor.execute(query)
    t1 =cursor.fetchall()
    df =pd.DataFrame(t1,columns =["Channel Name","Total Number OF View"])
    st.write(df)
    
    
    
elif questions ==  "8. the names of all the channels that have published videos in the year 2022":
    query ='''select c.channel_name as "Channel Name",v.video_name as "Video Name",v.Published_date as "Published Year"
                from channels c
                join videos v on c.channel_id = v.channel_id
                where v.Published_date between "2022-01-01" and "2023-01-01"'''
    
    cursor.execute(query)
    t1 =cursor.fetchall()
    df =pd.DataFrame(t1,columns =["Channel Name","Video Name","Published Year"])
    st.write(df)



elif questions == "9. the average duration of all videos in each channel, and what are their corresponding channel names":
    query ='''select c.channel_name as "Channel Name",sec_to_time((avg(v.Duration))) as "Avarage Duration"
            from videos v
            join channels c on c.channel_id = v.channel_id
            group by c.channel_id'''

    cursor.execute(query)
    t1 =cursor.fetchall()
    df =pd.DataFrame(t1,columns =["Channel Name","Avarage Duration"])
   
    t1 =[]
    for index,row in df.iterrows():
        c_n =row["Channel Name"]
        Avarage_Duration = str(row["Avarage Duration"])
        t1.append(dict(ChannelName =c_n,AvarageDuration=Avarage_Duration))
        
    df =pd.DataFrame(t1)
    st.write(df)
    
    
    
elif questions == "10. videos have the highest number of comments, and what are their  corresponding channel names":
    query ='''SELECT c.channel_name as"Channel Name" ,v.Comments_count "Number Of Comments"
            FROM channels c
            join videos v on c.channel_id = v.channel_id
            ORDER BY v.Comments_count DESC'''
    
    cursor.execute(query)
    t1 =cursor.fetchall()
    df =pd.DataFrame(t1,columns =["Channel Name", "Number Of Comments"])
    st.write(df)










# ch_list =[]
# db =client["Youtube_Details"]
# coll1 =db["Channel_Details"]
# for ch_data in coll1.find({},{'_id':0,"Channel_Information":1}):
#     ch_list.append(ch_data["Channel_Information"])
# df =pd.DataFrame(ch_list)

    

# sns.displot(data=df,x=df["Channel_Name"],)    
