import scrapy
from scrapy_splash import SplashRequest
import re
from scrapy import signals
import json
import uuid
import hashlib
import logging


class InstagramSpider(scrapy.Spider):
    SPLASH_INSTA_LOGIN = """
                                    function main(splash, args)
                                        splash.html5_media_enabled = true
                                        splash.private_mode_enabled = false
                                        splash:set_user_agent("Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko)  Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36")

                                        assert(splash:go(args.url))
                                        assert(splash:wait(5))

                                        splash:send_keys("<Tab>")
                                        splash:send_text("rocio_lampote")
                                        splash:send_keys("<Tab>")
                                        splash:send_text("rociolampote")
                                        splash:send_keys("<Return>")

                                        assert(splash:wait(5))

                                        return {
                                            html = splash:html(),
                                            cookies = splash:get_cookies()
                                        }
                                    end
                                """

    INSTAGRAM_LOGIN_PAGE = 'https://www.instagram.com/accounts/login/'
    SPLASH_GET_POSTS = """
                            function main(splash, args)
                                splash.html5_media_enabled = true
                                splash.private_mode_enabled = false
                                splash:init_cookies(splash.args.cookies)
                                splash:set_user_agent("Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko)  Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36")
                                
                                assert(splash:go(args.url))
                                
                                assert(splash:wait(10))
                                local scroll_to = splash:jsfunc("window.scrollTo")

                                local results = {}

                                for i=0, 1 do
                                    assert(splash:wait(0.2))
                                    results[i] = splash:html()
                                    scroll_to(0, i*800)
                                end

                                return {
                                    allPages = results,
                                    cookies = splash:get_cookies(),
                                    account = args.account
                                }
                            end
                          """
    SPLASH_INSTA_GET_COMMENTS = """
                                function main(splash, args)
                                    splash.html5_media_enabled = true
                                    splash.private_mode_enabled = false
                                    splash:init_cookies(splash.args.cookies)
                                    splash:set_user_agent("Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko)  Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36")

                                    assert(splash:go(args.url))
                                    
                                    assert(splash:wait(2))

                                    for i=0, 1400 do
                                        local b = splash:select('span[aria-label="Load more comments"]')
                                        if (b ~= nil) then
                                            assert(b:mouse_click{})
                                        else
                                            assert(splash:wait(0.1))
                                        end
                                    end

                                    return {
                                        html = splash:html(),
                                        cookies = splash:get_cookies(),
                                        account = args.account,
                                        post = args.post
                                    }
                                end
                                """
    SPLASH_INSTA_GET_ACCOUNT = """
                                function main(splash, args)
                                    splash.html5_media_enabled = true
                                    splash.private_mode_enabled = false
                                    splash:init_cookies(splash.args.cookies)
                                    splash:set_user_agent("Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko)  Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36")
                                    
                                    assert(splash:go(args.url))
                                    assert(splash:wait(10))

                                    return {
                                        html = splash:html(),
                                        cookies = splash:get_cookies(),
                                        account = args.account
                                    }
                                end
                                """
    SPLASH_INSTA_GET_LIKES = """
                                function main(splash, args)
                                    splash.html5_media_enabled = true
                                    splash.private_mode_enabled = false
                                    splash:init_cookies(splash.args.cookies)
                                    splash:set_user_agent("Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48B; wv) AppleWebKit/537.36 (KHTML, like Gecko)  Version/4.0 Chrome/43.0.2357.65 Mobile Safari/537.36")

                                    assert(splash:go(args.url))
                                    assert(splash:wait(3))
                                    
                                    local elements  = splash:select_all('button._8A5w5[type*="button"]')
                                    local last
                                    
                                    for i, element in ipairs(elements) do
                                        last = element
                                    end
                                    
                                    if last ~= nil then
                                        assert(last:mouse_click{})
                                    end
                                    
                                    assert(splash:wait(3))

                                    for i=0, 7000 do
                                        splash:send_keys("<Tab>")
                                        assert(splash:wait(0.1))
                                    end

                                    return {
                                        html = splash:html(),
                                        cookies = splash:get_cookies(),
                                        account = args.account,
                                        post = args.post
                                    }
                                end
                                """
    name = 'instagram'
    allowed_domains = ['instagram.com']
    # accounts_to_scrap = ['cerveceriaantares',
    #                      'antaresmdpguemes',
    #                      'cervezabaum',
    #                      'gluckcerveceria',
    #                      'hops.recova',
    #                      'cheverry.olavarria',
    #                      'lapalomabrewing',
    #                      'alardeolavarria',
    #                      'alardeolavarria',
    #                      'oghamalvarado',
    #                      'puntobohr',
    #                      'beerlin.cerveceria',
    #                      'bruderbg',
    #                      'gewerbeerbar',
    #                      'cervecerialebron',
    #                      'talantemdp']

    accounts_to_scrap = ['cervezabaum']

    custom_settings = {
        'ROBOTSTXT_OBEY': False
    }

    def start_requests(self):
        yield SplashRequest(self.INSTAGRAM_LOGIN_PAGE,
                            self.instagram_login_callback,
                            endpoint="execute",
                            args={'lua_source': self.SPLASH_INSTA_LOGIN})

    def instagram_login_callback(self, response):
        not_logged = re.findall('not-logged-in', response.body.decode('utf-8'))
        if not not_logged:
            for account in self.accounts_to_scrap:
                yield SplashRequest('https://www.instagram.com/' + account + '/',
                                    self.parse_page,
                                    endpoint="execute",
                                    args={'account': account, 'lua_source': self.SPLASH_GET_POSTS, 'timeout': 90.0})
        else:
            logging.critical('NOT LOGGUED IN: ' + response.body.decode('utf-8'))

    def parse_page(self, response):

        account = self.extract_account(response)
        yield account

        posts = self.extract_posts_list(response)
        posts = posts[: 1]
        print("=======")
        print(posts)
        print("=======")

        for post in posts:
            yield SplashRequest('https://www.instagram.com/p/' + post + '/',
                                self.parse_comments,
                                endpoint="execute",
                                args={'post': post, 'account': response.data['account'], 'lua_source': self.SPLASH_INSTA_GET_COMMENTS, 'timeout': 3600.0})

            yield SplashRequest('https://www.instagram.com/p/' + post + '/#likes',
                                self.parse_likes,
                                endpoint="execute",
                                args={'post': post, 'account': response.data['account'], 'lua_source': self.SPLASH_INSTA_GET_LIKES, 'timeout': 3600.0})

    def parse_comments(self, response):

        post_details = self.extract_post_details(response)

        yield post_details

        for ht in self.extract_hashtags(post_details['text'], post_details['postId']):
            yield ht

        comments = self.extract_comments(response)

        for comment in comments:
            yield SplashRequest('https://www.instagram.com/' + comment['commentator'] + '/',
                                self.parse_account,
                                endpoint="execute",
                                args={'account': comment['commentator'], 'lua_source': self.SPLASH_INSTA_GET_ACCOUNT, 'timeout': 90.0})
            yield comment

    def parse_likes(self, response):
        likes = self.extract_likes(response)

        for account in likes:
            yield SplashRequest('https://www.instagram.com/' + account + '/',
                                self.parse_account,
                                endpoint="execute",
                                args={'account': account, 'lua_source': self.SPLASH_INSTA_GET_ACCOUNT, 'timeout': 90.0})
            hash_object = hashlib.sha1(
                bytes(response.data["post"] + account, 'utf-8'))
            like_id = str(hash_object.hexdigest())
            yield {
                "likeId": like_id,
                "likerId": account,
                "postId": response.data["post"],
                "type": "like"
            }

    def parse_account(self, response):
        yield self.extract_account(response)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(InstagramSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        spider.logger.info('Spider closed: %s', spider.name)

    def extract_account(self, response):
        followers_public = response.xpath(
            '//body/div/section/main/div/header/section/ul/li[2]/a/span[@title]/@title').extract()
        followers_private = response.xpath(
            '//body/div/section/main/div/header/section/ul/li[2]/span/span[@title]/@title').extract()

        following_raw = response.xpath(
                '//body/div/section/main/div/header/section/ul/li[3]/a/span/text()').extract()

        posts_raw = response.xpath(
            '//body/div/section/main/div/header/section/ul/li/span/span/text()').extract()

        if posts_raw:
            posts = int(re.sub('[^0-9]', '', posts_raw[0]))
        else:
            posts = -1

        if followers_public:
            followers = int(re.sub('[^0-9]', '', followers_public[0]))
            if following_raw:
                following = int(re.sub('[^0-9]', '', following_raw[0]))
            else:
                following = -1
            private = False
        else:
            if followers_private:
                followers = int(followers_private[0].replace(',', ''))
                if following_raw:
                    following = int(re.sub('[^0-9]', '', response.xpath(
                        '//body/div/section/main/div/header/section/ul/li[3]/span/span/text()').extract()[0]))
                else:
                    following = -1
                private = True
            else:
                followers = -1
                following = -1
                private = True

        bio_raw = response.xpath(
            "//div[contains(@class, '-vDIg')]//text()").extract()

        if bio_raw:
            bio = ''
            for b in bio_raw:
                bio += b
        else:
            bio = '?'

        return {
            "accountId": response.data['account'],
            "followers": followers,
            "posts": posts,
            "following": following,
            "bio": bio,
            "private": private,
            "type": "account"
        }

    def extract_posts_list(self, response):
        posts_raw = list(dict.fromkeys(re.findall(
            '"\/p\/\w+-?\/', response.body.decode('utf-8'))))
        posts = [post.replace(
            '"/p/', '').replace('/', '') for post in posts_raw]

        return posts

    def extract_post_details(self, response):

        date_raw = response.xpath(
            "//time[contains(@class, '_1o9PC')]/@datetime").extract()

        if date_raw:
            date = date_raw[0]
        else:
            date = '?'

        text_raw = response.xpath(
            "//body/div/section/main/div/div/article/div/div/ul/div/li/div/div/div[contains(@class, 'C4VMK')]/span//text()").extract()

        if text_raw:
            text = ''
            for tr in text_raw:
                text += tr
        else:
            text = '?'

        return {
            "postId": response.data['post'],
            "accountId": response.data['account'],
            "date": date,
            "text": text,
            "type": "post"
        }

    def extract_hashtags(self, post_text, post_id):
        hastag_list = list(dict.fromkeys(re.findall('#\w+',
                                                    post_text)))

        hastag_list_return = []

        for ht in hastag_list:
            hastag_list_return.append({
                "hashtag": ht,
                "postId": post_id,
                "type": "hastag"
            })

        return hastag_list_return

    def extract_comments(self, response):

        comments = []
        count = 1

        while True:

            base = '//body/div/section/main/div/div/article/div/div/ul/ul[' + str(
                count) + ']'

            commentator_raw = response.xpath(
                base + '/div/li/div/div/div/h3/div/span/a/text()').extract()

            if commentator_raw:
                commentator = commentator_raw[0]
            else:
                break

            comment_date_raw = response.xpath(
                base + '/div/li/div/div/div/div/div/a/time/@datetime').extract()

            if comment_date_raw:
                comment_date = comment_date_raw[0]
            else:
                comment_date = '?'

            comment_text_raw = response.xpath(
                base + '/div/li/div/div/div/span/text()').extract()

            if comment_text_raw:
                comment_text = comment_text_raw[0]
            else:
                comment_text = str(uuid.uuid4())

            hash_object = hashlib.sha1(bytes(comment_text, 'utf-8'))
            comment_hash = str(hash_object.hexdigest())

            comments.append({
                "commentId": comment_hash,
                "commentText": comment_text,
                "commentDate": comment_date,
                "commentator": commentator,
                "postId": response.data['post'],
                "type": "comment"
            })

            count += 1

        return comments

    def extract_likes(self, response):
       return  response.xpath(
            '//body/div/div/div/div/div/div/div/div/div/div/span/a/@title').extract()
