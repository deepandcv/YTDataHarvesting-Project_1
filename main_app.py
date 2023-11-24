import googleapiclient
from googleapiclient import discovery
import pymongo
from pprint import pprint
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
import streamlit as st
import pandas as pd
import time as t
from PIL import Image

# channel ids
Apna_college = chid_01 =  "UCBwmMxybNva6P_5VmxjzwqA"        #  Apna College               
Vj_Siddhu_Vlogs = chid_02 =  "UCJcCB-QYPIBcbKcBQOTwhiA"     # Vj Siddhu Vlogs
PR_Sundar = chid_03 =  "UCS2NdYUmv_PUyyKeDAo5zYA"           # P R Sundar
Sundas_Khalid = chid_04 =  "UCteRPiisgIoHtMgqHegpWAQ"       # Sundas Khalid
Just_the_Watch = chid_05 =  "UCwNq5wCP71o59cQ2qyT5fXw"      # Just the Watch

#Api key 01 developerKey
api_key01='AIzaSyBA_Q4oa2-5qfUxBlnNEKZEsZzEs4KXHok'  
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key01)  

st.set_page_config(page_title="Streamlit App", page_icon="random", layout="wide", initial_sidebar_state="auto",
                   menu_items={'Get Help': "https://docs.streamlit.io/"})

st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
st.header(":red[Using SQL, MongoDB and Streamlit]", divider='grey')
select_channel = st.selectbox("Select a Channel", ["Select Channel ID",Apna_college, Vj_Siddhu_Vlogs, PR_Sundar, Sundas_Khalid, Just_the_Watch])

def channel1(select_channel):
  request = youtube.channels().list(
  part="snippet,contentDetails,statistics",
  id=select_channel
    )
  response = request.execute()
  request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=select_channel,
        maxResults=10
    )
  respons = request.execute()
  channel_1 = dict(Channel_Title= response['items'][0]['snippet']['localized']['title'],                    
                      Channel_SubcriberCount = response['items'][0]['statistics']['subscriberCount'],
                      Channel_videoCount= response['items'][0]['statistics']['videoCount'],
                      Channel_viewCount= response['items'][0]['statistics']['viewCount'])  
  return channel_1

if select_channel!= "Select Channel ID" and st.button('Extract Data'):
  X = channel1(select_channel)
  with st.spinner("Extracting Data Please Wait"):
    t.sleep(2)
  st.write("You Selected:")

  for key, value in X.items():
    st.write(f"{key} = {repr(value)}")
  st.button('clear') 


def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data

def get_playlist_info(channel_id):
    All_data = []

    request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=10,           
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
    return All_data    

def get_channel_videos(channel_id):
    video_ids = []
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 10)
    res = res.execute()                
        
    for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
    return video_ids

def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

def get_comment_info(video_ids):
        Comment_Information = []
     
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                    part = "snippet",
                    videoId = video_id,
                    maxResults = 10
                    )
            response5 = request.execute()
            
            for item in response5["items"]:
              comment_information = dict(
                      Comment_Id = item["snippet"]["topLevelComment"]["id"],
                      Video_Id = item["snippet"]["videoId"],
                      Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                      Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                      Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

              Comment_Information.append(comment_information)    
        return Comment_Information

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vid_details = get_video_info(vi_ids)
    comm_details = get_comment_info(vi_ids)

    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    mydb = myclient["youtube_database"]
    mycol = mydb["channel_collection"]
    mycol.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vid_details,
                     "comment_information":comm_details})
    
    return "upload completed successfully"

col1, col2 = st.columns(2)
if col1.button('Load Data Into MongoDB'):
  with st.spinner("Loading Data Please Wiat"):
    t.sleep(5)
    channel_details(select_channel)

def channel_info():
  v = []
  myclient = pymongo.MongoClient("mongodb://localhost:27017")
  mydb = myclient["youtube_database"]
  mycol = mydb["channel_collection"]
  for i in mycol.find({}, {"_id": 0, "channel_information": 1}):
    v.append(i['channel_information'])
  df = pd.DataFrame(v)

# mysql connection
  host = "localhost"
  user = "root"
  password = "password"

  connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password
    )

  mycursor = connection.cursor()

  mycursor.execute("CREATE DATABASE IF NOT EXISTS you_tube")
  mycursor.execute('USE you_tube') 
  drop_query = "DROP TABLE IF EXISTS channels"
  mycursor.execute(drop_query)
  create_table = """CREATE TABLE IF NOT EXISTS channel_info(
                    Channel_Id varchar(80) ,
                    Channel_Name varchar(100),
                    Subscription_Count bigint,
                    Views bigint,
                    Total_Videos int,
                    Channel_Description TEXT,
                    Playlist_Id varchar(80)
                )"""
  mycursor.execute(create_table)

  insert_info = """INSERT INTO channel_info(
                    Channel_Id,
                    Channel_Name,
                    Subscription_Count,
                    Views,
                    Total_Videos,
                    Channel_Description,
                    Playlist_Id
                ) VALUES(%s,%s,%s,%s,%s,%s,%s)"""

# Convert DataFrame to list of tuples for executemany
  data_to_insert = df.to_records(index=False).tolist()

# Use executemany to insert multiple rows
  mycursor.executemany(insert_info, data_to_insert)

# Commit the changes and close the connection
  connection.commit()
  mycursor.close()

def video_info():
  host = "localhost"
  user = "root"
  password = "password"

  connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password )
  
  mycursor = connection.cursor()
  mycursor.execute('USE you_tube')
  drop_query = "DROP TABLE IF EXISTS videos"
  mycursor.execute(drop_query)
  create_query = '''
    CREATE TABLE IF NOT EXISTS videos (
        Channel_Name varchar(150),
        Channel_Id varchar(100) ,
        Video_Id varchar(40)  , 
        Title TEXT, 
        Tags TEXT,
        Thumbnail varchar(225),
        Description text, 
        Published_Date varchar(50),
        Duration varchar(50), 
        Views INT, 
        Likes INT,
        Comments INT,
        Favorite_Count INT, 
        Definition TEXT, 
        Caption_Status varchar(50)
    )'''
  mycursor.execute(create_query)
  connection.commit()

  vi_list = []
  myclient = pymongo.MongoClient("mongodb://localhost:27017")
  mydb = myclient["youtube_database"]
  mycol = mydb["channel_collection"]

  for vi_data in mycol.find({}, {"_id": 0, "video_information": 1}):
    for i in range(len(vi_data["video_information"])):
        vi_list.append(vi_data["video_information"][i])

  df_2 = pd.DataFrame(vi_list)

  for index, row in df_2.iterrows():
    # Convert potential list values to strings
    row = row.apply(lambda x: ', '.join(x) if isinstance(x, list) else x)                   #------------------------------------

    insert_query = '''
                        INSERT INTO videos (
                        Channel_Name,
                        Channel_Id,
                        Video_Id, 
                        Title, 
                        Tags,
                        Thumbnail,
                        Description, 
                        Published_Date,
                        Duration, 
                        Views, 
                        Likes,
                        Comments,
                        Favorite_Count, 
                        Definition, 
                        Caption_Status ) 
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )'''

    values = (
                row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status']
            )

   
  mycursor.execute(insert_query, values)
  connection.commit()
  # print("Inserted successfully.")    
  mycursor.close()
  connection.close()

def com_info():

  myclient = pymongo.MongoClient("mongodb://localhost:27017")
  mydb = myclient["youtube_database"]
  mycol = mydb["channel_collection"]
  host = "localhost"
  user = "root"
  password = "password"

  connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password )

  mycursor = connection.cursor()
  mycursor.execute('use you_tube')
  drop_query = "DROP TABLE IF EXISTS comments"
  mycursor.execute(drop_query)
  create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published varchar(50))'''
  mycursor.execute(create_query)

  comm_list = []
    
  for com_data in mycol.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            comm_list.append(com_data["comment_information"][i])
  df_3 = pd.DataFrame(comm_list)


  for index, row in df_3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                      Video_Id ,
                                      Comment_Text,
                                      Comment_Author,
                                      Comment_Published)
                VALUES (%s, %s, %s, %s, %s)'''
            
  data_to_insert = df_3.to_records(index=False).tolist()           
  mycursor.executemany(insert_query, data_to_insert)
  connection.commit()
  mycursor.close()
  connection.close()          
 
if col2.button('Migrate Data into Mysql'):
  with st.spinner("Migrating Data Please Wiat"):
    t.sleep(5)
    channel_info()
    video_info()
    com_info()
      
st.header('Channel Analytics')

host = "localhost"
user = "root"
password = "password"

connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password )

mycursor = connection.cursor()
mycursor.execute('USE you_tube')

def Q1():          
  sql_query = "SELECT Title, Channel_Name FROM videos;"
  mycursor.execute(sql_query)
  result_set = mycursor.fetchall()

  X = pd.DataFrame(result_set,columns=[i[0] for i in mycursor.description])
  st.write(X)
  
  mycursor.close()
  connection.close()
 
def Q2():
  sql_query = """
    SELECT Channel_Name, COUNT(*) AS VideoCount
    FROM videos
    GROUP BY Channel_Name
    ORDER BY VideoCount ASC;    """
  
  mycursor.execute(sql_query)
  result_set = mycursor.fetchall()
  for row in result_set:
    channel_name, video_count = row
  st.write(f"Channel Name: {channel_name}, Video Count: {video_count}")
  mycursor.close()
  connection.close() 

def Q3():
  sql_query = """
    SELECT Title, Channel_Name, Views
    FROM videos
    ORDER BY Views DESC
    LIMIT 10;    """
  
  mycursor.execute(sql_query)
  result_set = mycursor.fetchall()
  X = pd.DataFrame(result_set,columns=[i[0] for i in mycursor.description])
  st.write(X)
  mycursor.close()
  connection.close()

def Q4():
  sql_query = """
    SELECT Channel_Name, Title,Comments
    FROM videos;    """
  
  mycursor.execute(sql_query)
  result_set = mycursor.fetchall()
  X = pd.DataFrame(result_set,columns=[i[0] for i in mycursor.description])
  st.write(X)
  mycursor.close()
  connection.close()

def Q5():
  sql_query = """
    SELECT Channel_Name, Title, Likes
    FROM videos
    ORDER BY Likes DESC
    LIMIT 1;    """
  
  mycursor.execute(sql_query)
  result_set = mycursor.fetchall()
  X = pd.DataFrame(result_set,columns=[i[0] for i in mycursor.description])
  st.write(X)
  mycursor.close()
  connection.close()

def Q6():
  sql_query = """
    SELECT Channel_Name, Title, Likes
    FROM videos;    """
  
  mycursor.execute(sql_query)
  result_set = mycursor.fetchall()
  X = pd.DataFrame(result_set,columns=[i[0] for i in mycursor.description])
  st.write(X)
  mycursor.close()
  connection.close()

def Q7():
  mycursor = connection.cursor()
  mycursor.execute('USE you_tube')
  sql_query = """
    SELECT Channel_Name, SUM(Views) AS TotalViews
    FROM videos
    GROUP BY Channel_Name;    """
  
  mycursor.execute(sql_query)
  result_set = mycursor.fetchall()
  X = pd.DataFrame(result_set,columns=[i[0] for i in mycursor.description])
  st.write(X)
  mycursor.close()
  connection.close()

def Q8():
  mycursor = connection.cursor()
  mycursor.execute('USE you_tube')
  sql_query = """
    SELECT DISTINCT Channel_Name
    FROM videos
    WHERE YEAR(Published_Date) = 2023;    """
  
  mycursor.execute(sql_query)
  result_set = mycursor.fetchall()
  for t in result_set:
    st.write(t)
  mycursor.close()
  connection.close()

def Q9():
  sql_query = """
    SELECT
    Channel_Name,
    AVG(
        TIME_TO_SEC(
            IF(POSITION('M' IN Duration) > 0,
                SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'T', -1) * 60,  0 )
                  +
            IF(POSITION('S' IN Duration) > 0, SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'M', -1), 0 )
        )
    ) as AverageDuration
    FROM videos
    GROUP BY Channel_Name;  """

  mycursor.execute(sql_query)
  result_set = mycursor.fetchall()
  X = pd.DataFrame(result_set,columns=[i[0] for i in mycursor.description])
  st.write(X)
  mycursor.close()
  connection.close()

def Q10():
  sql_query = """
    SELECT Channel_Name, Title, Comments
    FROM videos
    ORDER BY Comments DESC
    LIMIT 1;    """
  
  mycursor.execute(sql_query)
  result_set = mycursor.fetchall()
  X = pd.DataFrame(result_set,columns=[i[0] for i in mycursor.description])
  st.write(X)
  mycursor.close()
  connection.close()

selected_querry = st.selectbox("Select a Querry", ["select a  querry",
                  "What are the names of all the videos and their corresponding channels?",                            #1             
                  "Which channels have the most number of videos, and how many videos do they have?",                  #2     
                  "What are the top 10 most viewed videos and their respective channels?",                             #3     
                  "How many comments were made on each video, and what are their corresponding video names?",          #4 
                  "Which videos have the highest number of likes, and what are their corresponding channel names?",    #5
                  "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",   #6
                  "What is the total number of views for each channel, and what are their corresponding channel names?",            #7
                  "What are the names of all the channels that have published videos in the year 2022?",                            #8
                  "What is the average duration of all videos in each channel, and what are their corresponding channel names?",    #9
                  "Which videos have the highest number of comments, and what are their corresponding channel names"                #10                                    
                ])

if selected_querry == "What are the names of all the videos and their corresponding channels?":
  if st.button("Search"):    
    Q1()
elif selected_querry == "Which channels have the most number of videos, and how many videos do they have?":
  if st.button("Search"):
     Q2()
elif selected_querry == "What are the top 10 most viewed videos and their respective channels?":
  if st.button("Search"):
     Q3()
elif selected_querry == "How many comments were made on each video, and what are their corresponding video names?":
  if st.button("Search"):
     Q4()
elif selected_querry == "Which videos have the highest number of likes, and what are their corresponding channel names?":
  if st.button("Search"):
     Q5()
elif selected_querry == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
  if st.button("Search"):
     Q6()
elif selected_querry =="What is the total number of views for each channel, and what are their corresponding channel names?":
  if st.button("Search"):
     Q7()
elif selected_querry == "What are the names of all the channels that have published videos in the year 2022?":
  if st.button("Search"):
     Q8()
elif selected_querry == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
  if st.button("Search"):
     Q9()
elif selected_querry == "Which videos have the highest number of comments, and what are their corresponding channel names":
  if st.button("Search"):
     Q10()
st.button('Clear')

