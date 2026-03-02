import os
import logging
import boto3
from flask import Flask, jsonify, request, abort
from botocore.exceptions import ClientError
import pymysql

# Configure logging for CloudWatch
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Environment variables (set via EC2 user data or SSM)
DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')
S3_BUCKET = os.environ.get('S3_BUCKET', 'streamflow-anime-videos')

def get_db_connection():
    """Establish connection to RDS MySQL"""
    try:
        return pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def generate_presigned_url(video_key, expiration=3600):
    """Generate secure S3 pre-signed URL (valid for 1 hour)"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET, 'Key': video_key},
            ExpiresIn=expiration
        )
        return response
    except ClientError as e:
        logger.error(f"S3 URL generation error: {e}")
        return None

@app.route('/health', methods=['GET'])
def health_check():
    """ALB health check endpoint"""
    return jsonify({"status": "healthy"}), 200

@app.route('/api/videos/<int:video_id>', methods=['GET'])
def get_video(video_id):
    """Fetch video metadata and secure streaming URL"""
    try:
        # Get video metadata from RDS
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT title, s3_key FROM videos WHERE id = %s", (video_id,))
            video = cursor.fetchone()
        
        if not video:
            logger.warning(f"Video ID {video_id} not found")
            abort(404)

        # Generate secure S3 URL
        signed_url = generate_presigned_url(video['s3_key'])
        if not signed_url:
            abort(500)

        logger.info(f"Generated URL for video: {video['title']}")
        return jsonify({
            "id": video_id,
            "title": video['title'],
            "stream_url": signed_url
        })

    except Exception as e:
        logger.error(f"Error fetching video {video_id}: {str(e)}")
        abort(500)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
