from instascrape import Reel
import time

# session id
SESSIONID = "49551991610%3AWOkYqPgWXTPYxx%3A12%3AAYd0GHi4Md3vJq_XJLqbe4F7kA95xD9vLsZBAk-X0A"

# Header with session id
headers = {
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)\
	AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.74 \
	Safari/537.36 Edg/79.0.309.43",
	"cookie": f'sessionid={SESSIONID};'
}

# Passing Instagram reel link as argument in Reel Module
insta_reel = Reel(
	'https://www.instagram.com/reel/CKWDdesgv2l/?utm_source=ig_web_copy_link')

# Using scrape function and passing the headers
insta_reel.scrape(headers=headers)

# Giving path where we want to download reel to the
# download function
# insta_reel.download(fp=f".\\Desktop\\reel{int(time.time())}.mp4")

# printing success Message
print('Downloaded Successfully.')
