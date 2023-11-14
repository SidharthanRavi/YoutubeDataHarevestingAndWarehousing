import googleapiclient.discovery
from dotenv import load_dotenv
import os
import pandas as pd
import mysql.connector
import pymongo
import streamlit as st
from streamlit_option_menu import option_menu
import traceback

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

#Loading Environment file.
load_dotenv()
 
#getting api Key from environment file.  AND DB connections
api_key = os.getenv("googleYouTubeApiKey")
mongoDb_pwd=os.getenv("mangoDBpwd")
connection_string=f"mongodb+srv://sidharthan2908:{mongoDb_pwd}@cluster0.b7ydltv.mongodb.net/?retryWrites=true&w=majority"
MongoCon = pymongo.MongoClient(connection_string)
sqlDB = mysql.connector.connect(host='localhost',user='root', password='root',database='youtube')


#Function to Make Youtube Data Api V3 connection.
def youTubeApi_connect():
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(api_service_name,api_version, developerKey=api_key)
    return youtube
#-------------------------END of a Function-----------------------------------------------------

def streamlit_config():

    # page configuration
    st.set_page_config(page_title="Youtube Data Harevesting and Warehousing", page_icon=":guardsman:", layout="wide")

    # Project title - Youtube Data Harevesting and WareHousing 
    st.title("Youtube Data Harevesting and Warehousing")

    st.markdown(f'<h6 style="text-align: right;"> by Sidharthan R</h6>', unsafe_allow_html=True)
    st.divider()
#-------------------------END of a Function-----------------------------------------------------

#Youtube extract class which will extract the data from youtube api
class youtubeDataExtract:
    
    #Method  to Fetch the Channel Details like Subscription count, channel views, description etc..
    def channelDetailsApi(youtube, channel_id):
        request = youtube.channels().list(part='contentDetails, snippet, statistics, status',id=channel_id)
        response = request.execute()
        data = {'channel_name': response['items'][0]['snippet']['title'],
                'channel_id': response['items'][0]['id'],
                'subscription_count': response['items'][0]['statistics']['subscriberCount'],
                'channel_views': response['items'][0]['statistics']['viewCount'],
                'channel_description': response['items'][0]['snippet']['description'],
                'upload_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                'country': response['items'][0]['snippet'].get('country', 'Not Available')}
        return data
    #-------------------------END of a Method-----------------------------------------------------

    #Method To Fetch All the Video id Details
    def videoIdsList(youtube, upload_id):
        videoIDs_list = []
        nextPageToken = None
        NextPagePresent = True
        while NextPagePresent:

            request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=upload_id,
            maxResults=50,
            pageToken=nextPageToken
            )
            response = request.execute()

            for i in range(0, len(response['items'])):
                videoIDs_list.append(response['items'][i]['contentDetails']['videoId'])
            nextPageToken = response.get('nextPageToken')

            if nextPageToken is None:
                NextPagePresent=False
        return videoIDs_list
    #-------------------------END of a Method-----------------------------------------------------

    #Method  the details of the playlists in the youtube channel id provided by the user
    def playlist(youtube, channel_id, upload_id):
        playlist_details = []
        nextPageToken = None
        nextPage = True
        while nextPage:

            request = youtube.playlists().list(part="snippet,contentDetails,status",channelId=channel_id,maxResults=50,pageToken=nextPageToken)
            response = request.execute()

            for i in range(0, len(response['items'])):
                data = {'playlist_id': response['items'][i]['id'],
                    'playlist_name': response['items'][i]['snippet']['title'],
                    'channel_id': channel_id,
                    'upload_id': upload_id}
                playlist_details.append(data)
            nextPageToken = response.get('nextPageToken')
            if nextPageToken is None:
                nextPage=False
        return playlist_details
    #-------------------------END of a Method-----------------------------------------------------

    #Method To collect the Video details like video name, like count, comment count and more..
    def video(youtube, video_id, upload_id):

        request = youtube.videos().list(part='contentDetails, snippet, statistics',id=video_id)
        response = request.execute()

        caption = {'true': 'Available', 'false': 'Not Available'}

        # convert PT15M33S to 00:15:33 format using Timedelta function in pandas

        def time_duration(t):
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b

        videoData = {'video_id': response['items'][0]['id'],
                'video_name': response['items'][0]['snippet']['title'],
                'video_description': response['items'][0]['snippet']['description'],
                'upload_id': upload_id,
                'tags': response['items'][0]['snippet'].get('tags', []),
                'published_date': response['items'][0]['snippet']['publishedAt'][0:10],
                'published_time': response['items'][0]['snippet']['publishedAt'][11:19],
                'view_count': response['items'][0]['statistics']['viewCount'],
                'like_count': response['items'][0]['statistics'].get('likeCount', 0),
                'favourite_count': response['items'][0]['statistics']['favoriteCount'],
                'comment_count': response['items'][0]['statistics'].get('commentCount', 0),
                'duration': time_duration(response['items'][0]['contentDetails']['duration']),
                'thumbnail': response['items'][0]['snippet']['thumbnails']['default']['url'],
                'caption_status': caption[response['items'][0]['contentDetails']['caption']]}

        if videoData['tags'] == []:
            del videoData['tags']

        return videoData
    #-------------------------END of a Method-----------------------------------------------------

    #Method to Fetch the Comments Details like comment text, comment on and more 
    def comment(youtube, video_id):

        request = youtube.commentThreads().list(part='id, snippet',videoId=video_id,maxResults=100)
        response = request.execute()

        comment = []

        for i in range(0, len(response['items'])):
            data = {'comment_id': response['items'][i]['id'],
                    'comment_text': response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'comment_author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'comment_published_date': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][0:10],
                    'comment_published_time': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][11:19],
                    'video_id': video_id}
            
            comment.append(data)

        return comment
    #-------------------------END of a Method-----------------------------------------------------

    #youtube Extract Methods Main Method
    def main(channel_id,youtube):
        channel = youtubeDataExtract.channelDetailsApi(youtube, channel_id)
        upload_id = channel['upload_id']
        playlist = youtubeDataExtract.playlist(youtube, channel_id, upload_id)
        video_ids = youtubeDataExtract.videoIdsList(youtube, upload_id)

        video = []
        comment = []

        #for Each Video ids Extracted 
        for i in video_ids:
            v = youtubeDataExtract.video(youtube, i, upload_id)
            video.append(v)

            try:
                c = youtubeDataExtract.comment(youtube, i)
                comment.append(c)
            except:
                pass

        finalYoutubeData = {'channel': channel,
                 'playlist': playlist,
                 'video': video,
                 'comment': comment}

        return finalYoutubeData
    #-------------------------END of a Method-----------------------------------------------------

    #Method to return the Channel Details, for displaying in front end
    def displaySampleData(channel_id,youtube):

        channel = youtubeDataExtract.channelDetailsApi(youtube, channel_id)
        upload_id = channel['upload_id']
        playlist = youtubeDataExtract.playlist(youtube, channel_id, upload_id)
        video_ids = youtubeDataExtract.videoIdsList(youtube, upload_id)

        video = []
        comment = []

        for i in video_ids:
            v = youtubeDataExtract.video(youtube, i, upload_id)
            video.append(v)

            # skip disabled comments error in looping function
            try:
                c = youtubeDataExtract.comment(youtube, i)
                comment.append(c)
            except:
                pass
            break

        final = {'channel': channel,
                 'playlist': playlist,
                 'video': video,
                 'comment': comment}

        return final 
    #-------------------------END of a Method-----------------------------------------------------

#-------------------------END of a Youtube Extract Class------------------------------------------

#Class mongo is for Mongo Data Actions(Eg:- Mongo DB connection, Insert document, Fetch Document)
class mongodb:

    #Method to drop temp Collection, Temp Mongo DB stores the data temporarily till the Data is migrated to main Mongo DB
    def drop_temp_collection():

        db = MongoCon['YoutubeMongoDBTemp']
        col = db.list_collection_names()
        if len(col) > 0:
            for i in col:
                db.drop_collection(i)
    #-------------------------END of a Method-----------------------------------------------------

    #Method to Store the document in mongoDB
    def data_storage(channel_name, database, data):
        db = MongoCon[database]
        col = db[channel_name]
        col.insert_one(data)

    #-------------------------END of a Method-----------------------------------------------------

    #Main Method of the class
    def main(database):
        db = MongoCon['YoutubeMongoDBTemp']
        col = db.list_collection_names()

        if len(col) == 0:
            st.info("There is no data retrived from youtube")
        else:
            db = MongoCon['YoutubeMongoDBTemp']
            col = db.list_collection_names()
            channel_name = col[0]


            data_youtube = {}
            col1 = db[channel_name]
            for i in col1.find():
                data_youtube.update(i)

            # verify channel name already exists in database
            list_collection_names = mongodb.list_collection_names(database)

            if channel_name not in list_collection_names:
                mongodb.data_storage(channel_name, database, data_youtube)
                st.success("The data has been successfully stored in the MongoDB database")
                st.balloons()
                mongodb.drop_temp_collection()

            else:
                st.warning("The data has already been stored in MongoDB database")
                option = st.radio('Do you want to overwrite the data currently stored?',['Select one below', 'Yes', 'No'])

                if option == 'Yes':
                    db = MongoCon[database]

                    # delete existing data
                    db[channel_name].drop()

                    # add new data
                    mongodb.data_storage(channel_name, database, data_youtube)

                    st.success("The data has been successfully overwritten and updated in MongoDB db.")
                    st.balloons()
                    mongodb.drop_temp_collection()

                elif option == 'No':
                    mongodb.drop_temp_collection()
                    st.info("The data Migrate to Mongo DB has been stopped.")
    #-------------------------END of a Method-----------------------------------------------------

    #method to list the collection name
    def list_collection_names(database):
        db = MongoCon[database]
        col = db.list_collection_names()
        col.sort(reverse=False)
        return col
    #-------------------------END of a Method-----------------------------------------------------

    #Method  to print the Collections in MongoDB
    def order_collection_names(database):

        monRS = mongodb.list_collection_names(database)

        if monRS == []:
            st.info("The Mongodb database is currently empty")

        else:
            st.subheader('List of collections in MongoDB database')
            monRS = mongodb.list_collection_names(database)
            count = 1
            for i in monRS:
                st.write(str(count) + ' - ' + i)
                count += 1
    #-------------------------END of a Method-----------------------------------------------------

#-------------------------END of a Mongo db Class-------------------------------------------------


#Class sql is used for making SQL db connection, storing the Youtube Data, Migrating MongoDB to SQL table.
class sql:

    #create table method creates the data table if any of the necessary table is not present
    def create_tables():
        

        cursor = sqlDB.cursor()

        cursor.execute("create table if not exists channel(channel_id varchar(255) primary key,channel_name	varchar(255),subscription_count int,channel_views int,channel_description text,upload_id varchar(255),country varchar(255));")

        cursor.execute("create table if not exists playlist(playlist_id varchar(255) primary key,playlist_name varchar(255),channel_id varchar(255),upload_id varchar(255));")

        cursor.execute("create table if not exists video(video_id varchar(255) primary key,video_name varchar(255),video_description text,upload_id varchar(255),tags text,published_date date,published_time time,view_count int,like_count int,favourite_count int,comment_count int,duration time,thumbnail varchar(255),caption_status varchar(255));")
        
        cursor.execute("create table if not exists comment(comment_id varchar(255) primary key,comment_text text,comment_author varchar(255),comment_published_date date,comment_published_time time,video_id varchar(255));")
        
        sqlDB.commit()
    #-------------------------END of a Method-----------------------------------------------------

    #method to list the channel names
    def list_channel_names():

        cursor = sqlDB.cursor()
        cursor.execute("select channel_name from channel")
        sqlResultSet = cursor.fetchall()
        finalList = [i[0] for i in sqlResultSet]
        finalList.sort(reverse=False)
        return finalList
    #-------------------------END of a Method-----------------------------------------------------

    #Method to print the Channel names in SQL DB
    def order_channel_names():

        sqRS = sql.list_channel_names()

        if sqRS == []:
            st.info("The SQL database is currently empty")

        else:
            st.subheader("List of channels in SQL database")
            count = 1
            for i in sqRS:
                st.write(str(count) + ' - ' + i)
                count += 1
    #-------------------------END of a Method-----------------------------------------------------

    #method Comments
    def comment(database, channel_name):
        
        mongdb = MongoCon[database]
        col = mongdb[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'comment': 1}):
            data.extend(i['comment'][0])

        df = pd.DataFrame(data)
        df = df.reindex(columns=['comment_id', 'comment_text', 'comment_author',
                                 'comment_published_date', 'comment_published_time', 'video_id'])
        df['comment_published_date'] = pd.to_datetime(
            df['comment_published_date']).dt.date
        df['comment_published_time'] = pd.to_datetime(
            df['comment_published_time'], format='%H:%M:%S').dt.time
        return df
    #-------------------------END of a Method-----------------------------------------------------

    #method playlist
    def playlist(database, channel_name):

        mongdb = MongoCon[database]
        col = mongdb[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'playlist': 1}):
            data.extend(i['playlist'])

        df = pd.DataFrame(data)
        df = df.reindex(
            columns=['playlist_id', 'playlist_name', 'channel_id', 'upload_id'])
        return df

    #-------------------------END of a Method-----------------------------------------------------

    #Method Channel
    def channel(database, channel_name):

        mongdb = MongoCon[database]
        col = mongdb[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'channel': 1}):
            data.append(i['channel'])

        df = pd.DataFrame(data)
        df = df.reindex(columns=['channel_id', 'channel_name', 'subscription_count', 'channel_views',
                                 'channel_description', 'upload_id', 'country'])
        df['subscription_count'] = pd.to_numeric(df['subscription_count'])
        df['channel_views'] = pd.to_numeric(df['channel_views'])
        return df
    #-------------------------END of a Method-----------------------------------------------------


    #method video
    def video(database, channel_name):

        mongdb = MongoCon[database]
        col = mongdb[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'video': 1}):
            data.extend(i['video'])

        df = pd.DataFrame(data)
        df = df.reindex(columns=['video_id', 'video_name', 'video_description', 'upload_id',
                                 'tags', 'published_date', 'published_time', 'view_count',
                                 'like_count', 'favourite_count', 'comment_count', 'duration',
                                 'thumbnail', 'caption_status'])

        df['published_date'] = pd.to_datetime(df['published_date']).dt.date
        df['published_time'] = pd.to_datetime(
            df['published_time'], format='%H:%M:%S').dt.time
        df['view_count'] = pd.to_numeric(df['view_count'])
        df['like_count'] = pd.to_numeric(df['like_count'])
        df['favourite_count'] = pd.to_numeric(df['favourite_count'])
        df['comment_count'] = pd.to_numeric(df['comment_count'])
        df['duration'] = pd.to_datetime(
            df['duration'], format='%H:%M:%S').dt.time
        df['tags']=df['tags'].apply(','.join)
        return df

    #-------------------------END of a Method-----------------------------------------------------


    #sql class main method 
    def main(mdb_database, sql_database):
        
        # create necessary tables in sql if No table is present in the Database
        sql.create_tables()

        # mongodb and sql channel names
        mongoRS = mongodb.list_collection_names(mdb_database)
        sqlRS = sql.list_channel_names()

        if sqlRS==mongoRS==[]:
            st.info("Both Mongodb and SQL databases are currently empty")
        else:
            mongodb.order_collection_names(mdb_database)
            sql.order_channel_names()

            # remaining channel name for migration
            list_mongodb_notin_sql = ['Select one']
            mongoRS = mongodb.list_collection_names(mdb_database)
            sqlRS = sql.list_channel_names()

            # verify channel name not in sql
            for i in mongoRS:
                if i not in sqlRS:
                    list_mongodb_notin_sql.append(i)

            # channel name for user selection
            option = st.selectbox('Select a Channel name to migrate', list_mongodb_notin_sql)

            if option == 'Select one':
                col1, col2 = st.columns(2)
                with col1:
                    st.warning('Please select the channel')

            else:
                channel = sql.channel(mdb_database, option)
                playlist = sql.playlist(mdb_database, option)
                video = sql.video(mdb_database, option)
                comment = sql.comment(mdb_database, option)
                

                cursor = sqlDB.cursor()

                cursor.executemany(f"""insert into channel(channel_id, channel_name, subscription_count,channel_views, channel_description, upload_id, country) values(%s,%s,%s,%s,%s,%s,%s)""", channel.values.tolist())

                cursor.executemany(f"""insert into playlist(playlist_id, playlist_name, channel_id,upload_id) values(%s,%s,%s,%s)""", playlist.values.tolist())

                cursor.executemany(f"""insert into video(video_id, video_name, video_description,upload_id, tags, published_date, published_time, view_count, like_count, favourite_count, comment_count, duration, thumbnail,  caption_status)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", video.values.tolist())

                cursor.executemany(f"""insert into comment(comment_id, comment_text, comment_author,  comment_published_date, comment_published_time, video_id) values(%s,%s,%s,%s,%s,%s)""", comment.values.tolist())

                sqlDB.commit()
                st.success("Migrated Data Successfully to SQL Data Warehouse")
                st.balloons()
                sqlDB.close()
    #-------------------------END of a Method-----------------------------------------------------

#-------------------------END of a SQL db Class---------------------------------------------------

#Class SQL Queries. This Class Contains Set of 10 queries. used to display the output in front end
class sqlQueriesAndVisualise:

    #method to display the names of all the videos and their corresponding channels?
    def query1():
        cursor=sqlDB.cursor()
        # using Inner Join to join the tables
        cursor.execute(f'''select video.video_name, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_id
                            order by channel.channel_name ASC''')

        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Video Names', 'Channel Names'], index=i)
        
        data = data.rename_axis('S.No')

        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        st.dataframe(data)
    #-------------------------END of a Method-----------------------------------------------------

    #method : channels have the most number of videos, and how many videos do they have
    def query2():
        cursor=sqlDB.cursor()
        cursor.execute(f'''select distinct channel.channel_name, count(distinct video.video_id) as total
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by channel.channel_id
                        order by total DESC''')
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Channel Names', 'Total Videos'], index=i)

        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        st.dataframe(data)
    #-------------------------END of a Method-----------------------------------------------------

    #method : What are the top 10 most viewed videos and their respective channels?
    def query3():
        cursor=sqlDB.cursor()
        cursor.execute(f'''select distinct video.video_name, video.view_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            order by video.view_count DESC
                            limit 10''')
        
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Video Names', 'Total Views', 'Channel Names'], index=i)

        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        st.dataframe(data)

    #-------------------------END of a Method-----------------------------------------------------


    #method
    def query4():
        cursor=sqlDB.cursor()
        cursor.execute(f'''select video.video_name, video.comment_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_name
                            order by video.comment_count DESC''')
            
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Video Names', 'Total Comments', 'Channel Names'], index=i)

        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        st.dataframe(data)

    #-------------------------END of a Method-----------------------------------------------------


    #method
    def query5():
        cursor=sqlDB.cursor()
        cursor.execute(f'''select distinct video.video_name, channel.channel_name, video.like_count
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            where video.like_count = (select max(like_count) from video)''')
            
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Video Names', 'Channel Names', 'Most Likes'], index=i)

        data = data.reindex(columns=['Video Names', 'Most Likes', 'Channel Names'])
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        st.dataframe(data)

    #-------------------------END of a Method-----------------------------------------------------


    #method
    def query6():
        cursor=sqlDB.cursor()
        cursor.execute(f'''select distinct video.video_name, video.like_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_id
                            order by video.like_count DESC''')
            
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Video Names', 'Total Likes', 'Channel Names'], index=i)

        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
            
        st.dataframe(data)
    #-------------------------END of a Method-----------------------------------------------------


    #method
    def query7():
        cursor=sqlDB.cursor()
        cursor.execute(f'''select channel_name, channel_views from channel
                            order by channel_views DESC''')
            
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Channel Names', 'Total Views'], index=i)
            
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        st.dataframe(data)
    #-------------------------END of a Method-----------------------------------------------------


    #method 
    def query8(year):
        cursor=sqlDB.cursor()
        cursor.execute(f"""select distinct channel.channel_name, count(distinct video.video_id) as total
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            where extract(year from video.published_date) = '{year}'
                            group by channel.channel_id
                            order by total DESC""")
            
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Channel Names', 'Published Videos'], index=i)

        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
            
        st.dataframe(data)
    #-------------------------END of a Method-----------------------------------------------------


    #method
    def query9():
        cursor=sqlDB.cursor()
        cursor.execute(f'''select channel.channel_name, substring(cast(avg(video.duration) as varchar), 1, 8) as average
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by channel.channel_id
                            order by average DESC''')
            
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Channel Names', 'Average Video Duration'], index=i)
            
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        st.dataframe(data)
    #-------------------------END of a Method-----------------------------------------------------


    #method
    def query10():
        cursor=sqlDB.cursor()
        cursor.execute(f'''select video.video_name, video.comment_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.upload_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_name
                            order by video.comment_count DESC
                            limit 1''')
            
        s = cursor.fetchall()

        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(s, columns=['Video Names', 'Channel Names', 'Total Comments'], index=i)

        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

        st.dataframe(data)
    #-------------------------END of a Method-----------------------------------------------------


    #main method of sqlQueries function
    def main():

        st.write("## :orange[Select any question to get Insights]")
        questions = st.selectbox('Questions',
            ['1. What are the names of all the videos and their corresponding channels?',
            '2. Which channels have the most number of videos, and how many videos do they have?',
            '3. What are the top 10 most viewed videos and their respective channels?',
            '4. How many comments were made on each video, and what are their corresponding video names?',
            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
            '6. What is the total number of likes for each video, and what are their corresponding video names?',
            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
            '8. What are the names of all the channels that have published videos in the year?',
            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
            '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

        if questions == '1. What are the names of all the videos and their corresponding channels?':
            sqlQueriesAndVisualise.query1()

        elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            sqlQueriesAndVisualise.query2()

        elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
            sqlQueriesAndVisualise.query3()

        elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
            sqlQueriesAndVisualise.query4()

        elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            sqlQueriesAndVisualise.query5()

        elif questions == '6. What is the total number of likes for each video, and what are their corresponding video names?':
            sqlQueriesAndVisualise.query6()

        elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            sqlQueriesAndVisualise.query7()

        elif questions == '8. What are the names of all the channels that have published videos in the year?':
            year = st.text_input('Enter the year')
            submit = st.button('Submit')
            if submit:
                sqlQueriesAndVisualise.query8(year)
        
        elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            sqlQueriesAndVisualise.query9()

        elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            sqlQueriesAndVisualise.query10()

    #-------------------------END of a Method-----------------------------------------------------

#-------------------------END of a SQL query Class------------------------------------------------


#Actual flow starts from here
streamlit_config()
st.write('')
st.write('')


with st.sidebar:
    

    option = option_menu(menu_title='Steps For Youtube Data Extraction and Warehousing', options=['Data Retrive from YouTube API','Store data to MongoDB','Migrating Data to SQL', 'SQL Queries', 'Exit'],
                         icons=['youtube', 'database-add', 'database-fill-check', 'list-task', 'sign-turn-right-fill'])


if option == 'Data Retrive from YouTube API':

    try:

        # get input from user
        channel_id = st.text_input("Enter Channel ID: ")
        
        submit = st.button(label='Submit')

        if submit and option is not None:
            
            youtube = youTubeApi_connect()
            st.write(channel_id)
            data = {}
            final = youtubeDataExtract.main(channel_id,youtube)
            data.update(final)
            st.write(data)
            channel_name = data['channel']['channel_name']

            mongodb.drop_temp_collection()
            mongodb.data_storage(channel_name=channel_name,database='YoutubeMongoDBTemp', data=final)

            # display the sample data in streamlit
            st.json(youtubeDataExtract.displaySampleData(channel_id,youtube))
            st.success('Retrived data from YouTube successfully')
            st.balloons()

    except Exception as e:
        st.warning("Enter correct Channel ID")

elif option=='Store data to MongoDB':
    # Migrate the Temp data to Main mongo DB
    mongodb.main('MainYoutubeProjectDB')
    st.success('Retrived data from YouTube successfully And Stored to Mongo DB')
    st.balloons()

elif option == 'Migrating Data to SQL':
    sql.main(mdb_database='MainYoutubeProjectDB', sql_database='youtube')


elif option == 'SQL Queries':
    s1 = sql.list_channel_names()
    if s1 == []:
        st.info("The SQL database is currently empty")
    else:
        sqlQueriesAndVisualise.main()


elif option == 'Exit':
    mongodb.drop_temp_collection()
    sqlDB.close()
    st.success('DB connection closed')
    st.write('')
    st.write('')
    st.success('Thank you for your time. Exiting the application')
    st.balloons()
