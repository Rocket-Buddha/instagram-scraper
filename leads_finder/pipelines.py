# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import sqlite3

class SQLlitePipeline(object):

    def open_spider(self, spider):
        self.connection = sqlite3.connect("instagram_leads.db")

    def close_spider(self, spider):
        self.connection.close()


    def process_item(self, item, spider):
        
        cursor  = self.connection.cursor()
        if item.get('type') == 'account':
            cursor.execute('''
                INSERT OR REPLACE INTO accounts (account_id, posts, followers, following, private, bio) VALUES(?,?,?,?,?,?)
            ''', (
                item.get('accountId'),
                item.get('posts'),
                item.get('followers'),
                item.get('following'),
                item.get('private'),
                item.get('bio')
            ))
        elif item.get('type') == 'post':
            cursor.execute('''
                INSERT OR REPLACE INTO posts (post_id, account_id, post_text, post_date) VALUES(?,?,?,?)
            ''', (
                item.get('postId'),
                item.get('accountId'),
                item.get('text'),
                item.get('date'),
            ))
        elif item.get('type') == 'hastag':
            cursor.execute('''
                INSERT OR REPLACE INTO hashtags (post_id, hashtag) VALUES(?,?)
            ''', (
                item.get('postId'),
                item.get('hashtag').lower()
            ))
        elif item.get('type') == 'comment':
            cursor.execute('''
                INSERT OR REPLACE INTO comments (comment_id, comment_text, comment_date, commentator, post_id) VALUES(?,?,?,?,?)
            ''', (
                item.get('commentId'),
                item.get('commentText'),
                item.get('commentDate'),
                item.get('commentator'),
                item.get('postId')
            ))
        elif item.get('type') == 'like':
            cursor.execute('''
                INSERT OR REPLACE INTO likes (like_id, post_id, liker_id) VALUES(?,?,?)
            ''', (
                item.get('likeId'),
                item.get('postId'),
                item.get('likerId')
            ))
      
        self.connection.commit()
        return item