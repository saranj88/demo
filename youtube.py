from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st

#API KEY CONNECTION
def Api_connect():
  Api_id="AIzaSyC9yPrsixq3ou2CDQfL3SOzQBFHkxjT2IQ"
  api_service_name="youtube"
  api_version="v3"
  youtube=build( api_service_name,api_version,developerKey=Api_id)
  return youtube
youtube=Api_connect()

#get channel details:
def get_channel_info(channel_id):
  request=youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
  response = request.execute()
  for i in response['items']:
            data=dict(channel_Name=i['snippet']['title'],
            channel_ID=i['id'],
            Subscribers=i['statistics']['subscriberCount'],
            views=i['statistics']['viewCount'],
            total_videos=i['statistics']['videoCount'],
            channel_Description=i['snippet']['description'],
            playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
  return data

#get video details:
def get_video_ids(channel_ids):
   video_ids=[]
   response=youtube.channels().list(id=channel_ids,
                                 part='contentDetails').execute()
   playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
   next_page_token=None
   while True:
       response1=youtube.playlistItems().list(
                                       part='snippet',
                                       playlistId=playlist_id,
                                       maxResults=50,
                                       pageToken=next_page_token).execute()

       for i in range(len(response1['items'])):
           video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
       next_page_token=response1.get('nextpageToken')
       if next_page_token is None:
          break
   return video_ids

#VIDEO INFORMATION
def get_video_info(video_ids):
    video_data=[]
    for v_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=v_id)
        response=request.execute()
        
        for item in response['items']:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                    channel_Id=item['snippet']['channelId'],
                    video_id=item['id'],
                    Title=item['snippet']['title'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet']['description'],
                    Published_date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics']['likeCount'],
                    Comment=item['statistics'].get('commentCount'),
                    Favorite_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption'])
            video_data.append(data)
    return video_data    

#get comment information
def get_comment_info(video_ids):
   comment_data=[]
   try:
       for video_id in video_ids:
          request = youtube.commentThreads().list(
              part="snippet",
              videoId=video_id,
              maxResults=50)
          response = request.execute()

          for item in response['items']:
             data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                       video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                       comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                       comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                       comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
             comment_data.append(data)

   except:
    pass
   return comment_data

#get playlist_details
def get_playlist_info(channel_id):
   next_page_token=None
   all_data=[]
   while True:
       request=youtube.playlists().list(
           part='snippet,contentDetails',
           channelId=channel_id,
           maxResults=50,
           pageToken=next_page_token)
       response=request.execute()
       for item in response['items']:
          data=dict(playlist_Id=item['id'],
            Title=item['snippet']['title'],
            channel_Id=item['snippet']['channelId'],
            channel_name=item['snippet']['channelTitle'],
            publishedAt=item['snippet']['publishedAt'],
            video_count=item['contentDetails']['itemCount'])
          all_data.append(data)

       next_page_token=response.get('nextpageToken')
       if next_page_token is None:
         break
   return all_data

#upload to mongoDB
client=pymongo.MongoClient("mongodb+srv://saravananj882:12345@cluster0.npzoru2.mongodb.net/?retryWrites=true&w=majority")
db=client["youtube_data"]

def channel_details(channel_id):
  ch_details=get_channel_info(channel_id)
  pl_details=get_playlist_info(channel_id)
  vi_ids=get_video_ids(channel_id)
  vi_details=get_video_info(vi_ids)
  com_details=get_comment_info(vi_ids)

  coll1=db['channel_details']
  coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                    "video_information":vi_details,"comment_information":com_details})
  return "upload completed sucessfully"

def channels_table():
        import mysql.connector
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='youtube_data')

        print(mydb)
        mycursor = mydb.cursor(buffered=True)
        drop_query='''drop table if exists channels'''
        mycursor.execute(drop_query)
        mydb.commit()

        create_query='''create table if not exists channels(channel_Name varchar(100),
                                                        channel_ID varchar(80) primary key,
                                                        Subscribers bigint,
                                                        views bigint,
                                                        total_videos int,
                                                        channel_Description text,
                                                        playlist_Id varchar(80))'''
        mycursor.execute(create_query) 
        mydb.commit() 

        ch_list=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                ch_list.append(ch_data["channel_information"])
        df=pd.DataFrame(ch_list)  

        for index,row in df.iterrows():
                insert_query='''insert into channels(channel_Name,
                                                channel_ID,
                                                Subscribers,
                                                views,
                                                total_videos,
                                                channel_Description,
                                                playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
                                                
                values=(row['channel_Name'],
                        row['channel_ID'],
                        row['Subscribers'],
                        row['views'],
                        row['total_videos'],
                        row['channel_Description'],
                        row['playlist_Id'])
                mycursor.execute(insert_query,values)
                mydb.commit()                      

def playlists_table():
    import mysql.connector
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='youtube_data')

    print(mydb)
    mycursor = mydb.cursor(buffered=True)


    create_query='''create table if not exists playlists(playlist_Id varchar(100),
                                                    Title varchar(80),
                                                    channel_Id varchar(100),
                                                    channel_name varchar(100),
                                                    publishedAt timestamp,
                                                    video_count text
                                                    )'''
    mycursor.execute(create_query) 
    mydb.commit()

    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)   
    #insert playlist data
    for index,row in df1.iterrows():
        insert_query='''insert into playlists(playlist_Id,
                                            Title,
                                            channel_Id,
                                            channel_name,
                                            publishedAt,
                                            video_count)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
                                            
        values=(row['playlist_Id'],
                row['Title'],
                row['channel_Id'],
                row['channel_name'],
                row['publishedAt'],
                row['video_count'])
        mycursor.execute(insert_query,values)
        mydb.commit()    
                        
def videos_table():
    import mysql.connector
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtube_data')

    print(mydb)
    mycursor = mydb.cursor(buffered=True)
    drop_query='''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists videos(channel_Name varchar(100),
                                                    channel_Id varchar(80),
                                                    video_id varchar(80),
                                                    Title varchar(150),
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_date timestamp,
                                                    Duration time,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comment int,
                                                    Favorite_count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(15))'''
    mycursor.execute(create_query) 
    mydb.commit()  

    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)       

    for index,row in df2.iterrows():
        insert_query='''insert into videos(channel_Name,
                                            channel_Id,
                                            video_id,
                                            Title,
                                            Thumbnail,
                                            Description,
                                            Published_date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comment,
                                            Favorite_count,
                                            Definition,
                                            Caption_Status)
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                            
        values=(row['channel_Name'],
                row['channel_Id'],
                row['video_id'],
                row['Title'],
                row['Thumbnail'],
                row['Description'],
                row['Published_date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comment'],
                row['Favorite_count'],
                row['Definition'],
                row['Caption_Status'])
        mycursor.execute(insert_query,values)
        mydb.commit()  

def comments_table():
    import mysql.connector
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='youtube_data')

    print(mydb)
    mycursor = mydb.cursor(buffered=True)
    drop_query='''drop table if exists comments'''
    mydb.commit()


    create_query='''create table if not exists comments(comment_id varchar(100)primary key,
                                                    video_Id varchar(80),
                                                    comment_Text text,
                                                    comment_Author varchar(150),
                                                    comment_published timestamp
                                                    )'''
    mycursor.execute(create_query) 
    mydb.commit()   

    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)   

    #insert comments data
    for index,row in df3.iterrows():
        insert_query='''insert into comments(comment_id,
                                            video_Id,
                                            comment_Text,
                                            comment_Author,
                                            comment_published)
                                            
                                            values(%s,%s,%s,%s,%s)'''
                                            
        values=(row['comment_id'],
                row['video_Id'],
                row['comment_Text'],
                row['comment_Author'],
                row['comment_published'])
        mycursor.execute(insert_query,values)
        mydb.commit()                           

def tables():
    channels_table()
    videos_table()
    playlists_table()
    comments_table()
    
    return "tables created succussfully"

def show_channel_table():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)  
    return df

def show_playlist_table():
    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list) 
    return df1  

def show_video_table():
    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)  
    return df2

def show_comment_table():
    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)
    return df3

#streamlit part
with st.sidebar:
    st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
    
channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client['youtube_data']
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]['channel_ID'])
    
    if channel_id in ch_ids:
        st.success("Channel Details of given channel id already exists")
    else:
        insert=channel_details(channel_id)    
        st.success(insert)
if st.button("Migrate to sql"):
    Tables=tables()
    st.success(Tables)
    
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))    
    
if show_table=="CHANNELS":
    show_channel_table()
                    
elif show_table=="PLAYLISTS":
    show_playlist_table() 

elif show_table=="VIDEOS":
    show_channel_table()   
    
elif show_table=="COMMENTS":
    show_comment_table()                        

#SQL connection
import mysql.connector
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtube_data')

print(mydb)
mycursor = mydb.cursor(buffered=True)

question=st.selectbox("select your question",("1.All the videos and the channel name",
                                              "2.channels with most number of videos",
                                              "3. 10 most viewed video",
                                              "4.comments in each videos",
                                              "5. videos with highest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos each channel",
                                              "10. videos with highest number of comments"))

if question=="1.All the videos and the channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)
    
elif question=="2.channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels
               order by total_videos desc'''
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df1=pd.DataFrame(t2,columns=["video_name","no of videos"])
    st.write(df1)
    
elif question=="3. 10 most viewed video":
    query3='''select views as view,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df2=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df2)
    
elif question=="4.comments in each videos":
    query4='''select comment as no_comments,title as videotitle from videos where comment is not null'''
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df3=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df3)
    
elif question=="5. videos with highest likes":
    query5='''select title as videotitle,channel_name as channelname, likes as likecount
            from videos where likes is not null order by likes desc'''
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df4=pd.DataFrame(t5,columns=[" videotitle","channelname","likecount"]) 
    st.write(df4) 
    
elif question=="6. likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
            
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df5=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df5) 
    
elif question== "7. views of each channel":
    query7='''select channel_name as channelname,views as totalviews from channels'''
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df6=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df6) 
 
elif question== "8. videos published in the year of 2022":
    query8='''select Title as video_title,published_date as videorelease,channel_name as channelname from videos
            where extract(year from published_date)=2022'''
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df7=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"]) 
    st.write(df7)  
    
elif question=="9. average duration of all videos each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos
            group by channel_name'''
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df8=pd.DataFrame(t9,columns=["channel name","averageduration"])
    df8

    T9=[]
    for index,row in df8.iterrows():
        channel_title=row["channel name"]    
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
        df9=pd.DataFrame(T9)
        st.write(df9)
        
elif question== "10. videos with highest number of comments":
    query10='''select title as videotitle, channel_name as channelname,comment as comment
            from videos where comment is not null order by comment desc '''
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channel name","comment"]) 
    st.write(df10)                     

