# Tragedy

Tragedy is a powerful python abstraction-layer for Cassandra.

Out of the box it supports
    *    Models
    *    Time-Sorted-Indexes
    *    Mirroring
    *    Foreign Key Relationships
    *    and Server-Validation of Models.

## Example (twitter-demo)

	import tragedy
	client = tragedy.connect(['localhost:9160'])
	from tragedy import *

	twitty_cluster = Cluster('Twitter Cluster')
	twitty_keyspace = Keyspace('Twitty', bbqcluster)

	ALLTWEETS_KEY = '!ALLTWEETS!'

	class User(DictRow):
	    username = RowKey()
    
	    firstname = StringColumnSpec(required=False)
	    lastname = StringColumnSpec(required=False)
	    password = StringColumnSpec()
    
	    def follow(self, *one_or_more):
	        fol = Following(username=self)
	        for other in one_or_more:
	            fol.append(other)
	            FollowedBy(username=other).append(self).save()
	        fol.save()
    
	    def tweet(self, message):
	        newtweet = Tweet(author=self, message=message[:140]).save()
	        TweetsSent(by_username=self).append(newtweet).save()
	        tr = TweetsReceived(by_username=ALLTWEETS_KEY)
	        tr.append(newtweet).save()
	        for follower in self.get_followed_by():
	            follower.receive(newtweet)            
    
	    def receive(self, tweet):
	        TweetsReceived(by_username=self).append(tweet).save()
    
	class Tweet(DictRow):
	    uuid = RowKey(autogenerate=True)

	    message = StringColumnSpec()
	    author = ForeignKey(foreign_class=User, required=True)

	    @staticmethod
	    def get_recent_tweets(*args, **kwargs):
	        tr = TweetsReceived(by_username=ALLTWEETS_KEY)
	        return tr.load(*args, **kwargs).loadIterValues()

	class TweetsSent(TimeSortedIndex):
	    _default_spec = TimeForeignKey(foreign_class=Tweet, required=False)
	    by_username = RowKey(referenced_by=User, autoload_values=True)

	class TweetsReceived(TimeSortedIndex):
	    _default_spec = TimeForeignKey(foreign_class=Tweet, required=False)
	    by_username = RowKey(referenced_by=User, autoload_values=True)

	class Following(TimeSortedUniqueIndex):
	    _default_spec = TimeForeignKey(foreign_class=User, required=False)
	    username = RowKey(referenced_by=User)
    
	class FollowedBy(TimeSortedUniqueIndex):
	    _default_spec = TimeForeignKey(foreign_class=User, required=False)
	    username = RowKey(referenced_by=User)

	chuck = User(username='chuck', firstname='Chuck', 
				 password='gibson').save()
	peter = User(username='peter', firstname='Peter', 
				 password='secret').save()
	bob = User(username='bob', firstname='Bob', 
			   lastname='Peters', password='password').save()
	dave = User(username='dave', firstname='dave',
				password='test').save()
	merlin = User(username='merlin', firstname='merlin',
			    password='sunshine').save()

	peter.follow(chuck)
	bob.follow(chuck, dave)
	dave.follow(chuck)
	chuck.follow(bob)
	merlin.follow(peter, dave)

	print 'Chuck has these followers:', chuck.get_followed_by().values()
	print 'Bob follows', bob.get_following().values()
	chuck.tweet('140 characters')
	dave.tweet('making breakfast')
	chuck.tweet('sitting at home being bored')

	print 'Chuck sent', [ (x['message'], x['author']['username']) 
						for x in chuck.get_tweets_sent(count=5)]
	print 'Bob received', [x['message'] 
						for x in bob.get_tweets_received(count=5)]
	print 'ALL RECENT TWEETS', [x['message'] 
						for x in Tweet.get_recent_tweets(count=4)]
