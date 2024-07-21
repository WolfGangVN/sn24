import mysql.connector

class VideoStorage:
    def __init__(self) -> None:
        """
        Use mysql to storage video data with folowing schema
        {'id': 'Bq30vO3K4Lw', 'title': 'George Lucas get mad about Mara Jade', 'description': 'George Lucas get mad about Mara Jade', 'duration': 0, 'view_count': '4186486', 'download': ''}
        """
        self.conn = mysql.connector.connect(
            host="localhost",
            port="3306",
            user="root",
            password="root",
        )
        # check connection
        if self.conn.is_connected():
            print("Connected to MySQL")
        else:
            print("Failed to connect to MySQL")
        self.init_database()

        
    def init_database(self):
        cursor = self.conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS video_storage")
        cursor.execute("USE sn24")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS videos (id VARCHAR(255) PRIMARY KEY, title VARCHAR(255), description TEXT, duration INT, view_count INT, download TEXT, topic VARCHAR(255), expired BOOLEAN DEFAULT FALSE, downloaded BOOLEAN DEFAULT FALSE)"
        )

        # create index on topic column
        # create index if not exists
        # cursor.execute("CREATE INDEX IF NOT EXISTS topic_index ON videos (topic)")
        # cursor.execute("CREATE INDEX topic_index ON videos (topic)")
        self.conn.commit()
        cursor.close()
    
    def insert_video(self, video, topic):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO videos (id, title, description, duration, view_count, download, topic) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (video["id"], video["title"], video["description"], video["duration"], video["view_count"], video["download"], topic)
        )
        self.conn.commit()
        cursor.close()
    
    def insert_videos(self, videos, topic):
        # insert many ignore duplicate
        cursor = self.conn.cursor()
        cursor.executemany(
            "INSERT IGNORE INTO videos (id, title, description, duration, view_count, download, topic) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            [(video["id"], video["title"], video["description"], video["duration"], video["view_count"], video["download"], topic) for video in videos]
        )
        self.conn.commit()
        cursor.close()
        
    def fetch_all(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM videos")
        result = cursor.fetchall()
        cursor.close()
        # return list of dict
        return [dict(zip(cursor.column_names, row)) for row in result]
    
    def update_download_path(self, video_id, download_path):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE videos SET download=%s WHERE id=%s", (download_path, video_id))
        self.conn.commit()
        cursor.close()
