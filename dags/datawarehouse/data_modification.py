import logging

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cur, conn, schema, row):
    try:
        video_id = "video_id"
        if schema == "staging":
            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "View_Count", "Like_Count", "Comment_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
                """,
                row,
            )
        else:
            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "View_Count", "Like_Count", "Comment_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
                """,
                row,
            )
        conn.commit()
        logger.info(f"Inserted row with Video_ID: {row[video_id]}")
    except Exception as e:
        logger.error(
            f"Error inserting row with Video_ID: {row.get(video_id, 'unknown')}")
        raise e


def update_rows(cur, conn, schema, row):
    try:
        # staging
        if schema == "staging":
            video_id = "video_id"
            upload_date = "publishedAt"
            video_title = "title"
            video_views = "viewCount"
            likes_count = "likeCount"
            comments_count = "commentCount"
        # core
        else:
            video_id = "video_id"
            upload_date = "publishedAt"
            video_title = "title"
            video_views = "viewCount"
            likes_count = "likeCount"
            comments_count = "commentCount"

        cur.execute(
            f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %({video_title})s,
                "View_Count" = %({video_views})s, 
                "Like_Count" = %({likes_count})s, 
                "Comment_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """,
            row,
        )
        conn.commit()
        logger.info(f"Updated row with Video_ID: {row[video_id]}")
    except Exception as e:
        logger.error(
            f"Error updating row with Video_ID: {row[video_id]} - {e}")
        raise e


def delete_rows(cur, conn, schema, ids_to_delete):
    try:
        ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN {ids_to_delete};
            """
        )
        conn.commit()
        logger.info(f"Deleted rows with Video_IDs: {ids_to_delete}")
    except Exception as e:
        logger.error(
            f"Error deleting rows with Video_IDs: {ids_to_delete} - {e}")
        raise e
