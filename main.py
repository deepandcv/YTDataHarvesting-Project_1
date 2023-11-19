from googleapiclient.discovery import build
import pymongo
from pprint import pprint
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
import streamlit as st
import pandas as pd

# channel ids
chid_01 =  "UCBwmMxybNva6P_5VmxjzwqA"   #  Apna College
chid_02 =  "UCJcCB-QYPIBcbKcBQOTwhiA"  # Vj Siddhu Vlogs
chid_03 =  "UCS2NdYUmv_PUyyKeDAo5zYA"   # P R Sundar
chid_04 =  "UCteRPiisgIoHtMgqHegpWAQ"  # Sundas Khalid
chid_05 =  "UCwNq5wCP71o59cQ2qyT5fXw"   # Just the Watch

#Api key_01 developerKey
api_key01 = "AIzaSyBA_Q4oa2-5qfUxBlnNEKZEsZzEs4KXHok"
youtube = build("youtube", "v3", developerKey=api_key01)

select_channel = st.selectbox("Select a Channel",[chid_01, chid_02, chid_03, chid_04, chid_05])

#getting channel ids
def channel1(select_channel):
  request = youtube.channels().list(
  part="snippet,contentDetails,statistics",
  id=select_channel
    )
  response = request.execute()
  request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=select_channel,
        maxResults=25
  )
  respons = request.execute()
  channel_1 = dict(
                Channel_Title= response['items'][0]['snippet']['localized']['title'],
                channel_id = response['items'][0]['id'],
                Channel_Description= response['items'][0]['snippet']['description'],
                Channel_SubcriberCount = response['items'][0]['statistics']['subscriberCount'],
                Channel_videoCount= response['items'][0]['statistics']['videoCount'],
                Channel_viewCount= response['items'][0]['statistics']['viewCount'],
                playlist_id = [respons['items'][0]['id'],respons['items'][1]['id']]
                )
  return channel_1

if select_channel and st.button('Fetch'):
  a = channel1(select_channel)
  st.write('You selected:', a)
st.button('clear') 


#getting playlist ids
def channel_info(channel_id):
    
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


#getting video ids
def playlist_info(channel_id):
    All_data = []

    request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
           
            )
    response = request.execute()

    for item in response['items']: 
      data={
          'PlaylistId':item['id'],
          'Title':item['snippet']['title'],
          'ChannelId':item['snippet']['channelId'],
          'ChannelName':item['snippet']['channelTitle'],
          'PublishedAt':item['snippet']['publishedAt'],
          'VideoCount':item['contentDetails']['itemCount']
        }
      All_data.append(data)        
    return All_data
    

def channel_videos(channel_id):
    video_ids = []
    request = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    request = youtube.playlistItems().list(part = 'snippet',
                                      playlistId = playlist_id, 
                                      maxResults = 30
                                      )
    response = request.execute()                                  
                                       
        
    for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
    return video_ids

def video_info(video_ids):
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
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition']                    
                        )
            video_data.append(data)
    return video_data

#getting comment information
def comment_info(video_ids):
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
                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                        )

            Comment_Information.append(comment_information)        
        return Comment_Information


def channel_details(channel_id):
    ch_details = channel_info(channel_id)
    pl_details = playlist_info(channel_id)
    vi_ids = channel_videos(channel_id)
    vi_details = video_info(vi_ids)
    com_details = comment_info(vi_ids)
    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    mydb = myclient["YouTube_database"]
    mycol = mydb["channel_details"]
    mycol.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                     "comment_information":com_details})
    
    return "upload completed successfully"
if st.button('Insert Data Into MongoDB'):
  channel_details(select_channel)

# passing channel ids in arg. to get all the informations above
def channel_info():
  v = []

  # conecting to mongodb
  myclient = pymongo.MongoClient("mongodb://localhost:27017")
  mydb = myclient["YouTube_database"]
  mycol = mydb["channel_details"]
  for i in mycol.find({}, {"_id": 0, "channel_information": 1}):
    v.append(i['channel_information'])

  df = pd.DataFrame(v)

# connecting to Mysql workbench
  host = "localhost"
  user = "root"
  password = "Dcv@mysql07."

  connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password
)

  cursor = connection.cursor()

  cursor.execute("create database if not exists You_Tube4")
  cursor.execute('use You_Tube4') 
  drop_query = "drop table if exists channels"
  cursor.execute(drop_query)
  create_table = """create table if not exists channel_info(
                    Channel_Id varchar(80) ,
                    Channel_Name varchar(100),
                    Subscription_Count bigint,
                    Views bigint,
                    Total_Videos int,
                    Channel_Description TEXT,
                    Playlist_Id varchar(80)
                )"""
  cursor.execute(create_table)

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
  cursor.executemany(insert_info, data_to_insert)

# Commit the changes and close the connection
  connection.commit()


def video_info():  
  connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password
  )
  cursor = connection.cursor()
  cursor.execute('USE you_tube4')
  drop_query = "drop table if exists videos"
  cursor.execute(drop_query)
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
                    
  cursor.execute(create_query)
  connection.commit()

  vi_list = []
  myclient = pymongo.MongoClient("mongodb://localhost:27017")
  mydb = myclient["YouTube"]
  col = mydb["channel_details"]
  for vi_data in col.find({}, {"_id": 0, "video_information": 1}):
    for i in range(len(vi_data["video_information"])):
        vi_list.append(vi_data["video_information"][i])

  df2 = pd.DataFrame(vi_list)

  for index, row in df2.iterrows():
    # Convert potential list values to strings
    row = row.apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    insert_query = '''
                      INSERT INTO videos (
                          Channel_Name,
                          Channel_Id,
                          Video_Id, 
                          Title,                                                     
                          Description, 
                          Published_Date,
                          Duration, 
                          Views, 
                          Likes,
                          Comments,
                          Favorite_Count, 
                          Definition                           
                      ) VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                      )
                  '''

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

    try:
        cursor.execute(insert_query, values)
        connection.commit()
        print("Inserted successfully.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        print(f"Failed insert values: {values}")

  cursor.close()
  connection.close()


def com_info():
#   g = 7
  myclient = pymongo.MongoClient("mongodb://localhost:27017")
  db = myclient["YouTube"]
  mycol = db["channel_details"]
  host = "localhost"
  user = "root"
  password = "Dcv@mysql07."

  connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password
)

  cursor = connection.cursor()
  cursor.execute('use You_Tube4')
  drop_query = "drop table if exists comments"
  cursor.execute(drop_query)
  create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published varchar(50)
                       )'''
  cursor.execute(create_query)

  com_list = []
    
  for com_data in mycol.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
  df3 = pd.DataFrame(com_list)


  for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                      Video_Id ,
                                      Comment_Text,
                                      Comment_Author,
                                      Comment_Published)
                VALUES (%s, %s, %s, %s, %s)

            '''
  data_to_insert = df3.to_records(index=False).tolist()           
  cursor.executemany(insert_query, data_to_insert)
  connection.commit()
  cursor.close()
  connection.close()          
 
if st.button('mysql'):
  channel_info()
  video_info()
  com_info()

host = "localhost"
user = "root"
password = "Dcv@mysql07."

connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password
)

cursor = connection.cursor()
cursor.execute('USE you_tube')

# Creating Questions

def Q1():          
  sql_query = "SELECT Title, Channel_Name FROM videos;"
  cursor.execute(sql_query)
  result_set = cursor.fetchall()
# a = cursor.description
  a = pd.DataFrame(result_set,columns=[i[0] for i in cursor.description])
  st.write(a)

  cursor.close()
  connection.close()
if st.button('What are the names of all the videos and their corresponding channels?'):
  Q1() 

 #    --------------------------------

def Q2():
  sql_query = """
                  SELECT Channel_Name, COUNT(*) AS VideoCount
                  FROM videos
                  GROUP BY Channel_Name
                  ORDER BY VideoCount ASC;
              """
  cursor.execute(sql_query)
  result_set = cursor.fetchall()
  for row in result_set:
    channel_name, video_count = row
  st.write(f"Channel Name: {channel_name}, Video Count: {video_count}")
  cursor.close()
  connection.close() 

if st.button('Which channels have the most number of videos, and how many videos do they have?'):
      Q2() 

#    --------------------------------
    
def Q3():
  sql_query = """
                SELECT Title, Channel_Name, Views
                FROM videos
                ORDER BY Views DESC
                LIMIT 10;
            """
  cursor.execute(sql_query)
  result_set = cursor.fetchall()
  for row in result_set:
    title, channel_name, views = row
  st.write(f"Title: {title}, Channel Name: {channel_name}, Views: {views}")
  cursor.close()
  connection.close()
if st.button('What are the top 10 most viewed videos and their respective channels?'):
      Q3() 

#    --------------------------------

def Q4():
  sql_query = """
                  SELECT Channel_Name, Title,Comments
                  FROM videos;   
              """
  cursor.execute(sql_query)
  result_set = cursor.fetchall()
  a = pd.DataFrame(result_set,columns=[i[0] for i in cursor.description])
  st.write(a)
  cursor.close()
  connection.close()
if st.button('How many comments were made on each video, and what are theircorresponding video names?'):
      Q4() 

#    --------------------------------

def Q5():
  sql_query = """
                  SELECT Channel_Name, Title, Likes
                  FROM videos
                  ORDER BY Likes DESC
                  LIMIT 1;
              """
  cursor.execute(sql_query)
  result_set = cursor.fetchall()
  a = pd.DataFrame(result_set,columns=[i[0] for i in cursor.description])
  st.write(a)
  cursor.close()
  connection.close()
if st.button('Which videos have the highest number of likes, and what are their corresponding channel names?'):
  Q5() 

#    --------------------------------

def Q6():
  sql_query = """
                  SELECT Channel_Name, Title, Likes
                  FROM videos;   
              """
  cursor.execute(sql_query)
  result_set = cursor.fetchall()
  a = pd.DataFrame(result_set,columns=[i[0] for i in cursor.description])
  st.write(a)
  cursor.close()
  connection.close()
if st.button('What is the total number of likes and dislikes for each video, and what are their corresponding video names?'):
      Q6() 

#    --------------------------------

def Q7():
  cursor = connection.cursor()
  cursor.execute('USE you_tube')
  sql_query = """
                SELECT Channel_Name, SUM(Views) AS TotalViews
                FROM videos
                GROUP BY Channel_Name;
            """
  cursor.execute(sql_query)
  result_set = cursor.fetchall()
  a = pd.DataFrame(result_set,columns=[i[0] for i in cursor.description])
  st.write(a)
  cursor.close()
  connection.close()
if st.button('What is the total number of views for each channel, and what are their corresponding channel names?'):
      Q7() 

#    --------------------------------

def Q8():
  cursor = connection.cursor()
  cursor.execute('USE you_tube')
  sql_query = """
                SELECT DISTINCT Channel_Name
                FROM videos
                WHERE YEAR(Published_Date) = 2023;
            """
  cursor.execute(sql_query)
  result_set = cursor.fetchall()
  for t in result_set:
    st.write(t)
  cursor.close()
  connection.close()
if st.button('What are the names of all the channels that have published videos in the year2023?'):
      Q8() 

#    --------------------------------

def Q9():
  sql_query = """
                  SELECT Channel_Name, TIME_TO_SEC(Duration) as AverageDuration
                  FROM videos
                  GROUP BY Channel_Name;
              """
  cursor.execute(sql_query)
  result_set = cursor.fetchall()
  a = pd.DataFrame(result_set,columns=[i[0] for i in cursor.description])
  st.write(a)
  cursor.close()
  connection.close()
if st.button('What is the average duration of all videos in each channel, and what are their corresponding channel names?'):
      Q9() 
             
#    --------------------------------

def Q10():
  sql_query = """
                SELECT Channel_Name, Title, Comments
                FROM videos
                ORDER BY Comments DESC
                LIMIT 1;
            """
  cursor.execute(sql_query)
  result_set = cursor.fetchall()
  a = pd.DataFrame(result_set,columns=[i[0] for i in cursor.description])
  st.write(a)
  cursor.close()
  connection.close()
if st.button('Which videos have the highest number of comments, and what are their corresponding channel names?'):
    Q10() 
   
