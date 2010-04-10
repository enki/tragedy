# Tragedy

Tragedy is a powerful Python abstraction-layer for Cassandra.

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

	twitty_cluster = Cluster('Twitty Cluster')
	twitty_keyspace = Keyspace('Twitty', twitty_cluster)

	ALLTWEETS_KEY = '!ALLTWEETS!' # virtual user that receives all tweets

	class User(Model):
	    """A Model is stored and retrieved by its RowKey.
	       Every Model has exactly one RowKey and one or more other Fields"""
	    username = RowKey()
	    firstname = StringField(required=False)
	    lastname = StringField(required=False) # normally fields are mandatory
	    password = StringField()
    
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
    
	class Tweet(Model):
	    uuid = RowKey(autogenerate=True) # generate a UUID for us.
	    message = StringField()    
	    author = ForeignKey(foreign_class=User, required=True)

	    @staticmethod
	    def get_recent_tweets(*args, **kwargs):
	        tr = TweetsReceived(by_username=ALLTWEETS_KEY)
	        return tr.load(*args, **kwargs).loadIterValues()
    
	    def __repr__(self):
	        return '<%s> %s' % (self['author']['username'], self['message'])

	class TweetsSent(Index):
	    """An index is an ordered mapping from one RowKey to
	       many other Objects of a specific type."""
	    by_username = RowKey(linked_from=User, autoload_values=True)
	    targetmodel = ForeignKey(foreign_class=Tweet, compare_with='TimeUUIDType')

	class TweetsReceived(Index):
	    by_username = RowKey(linked_from=User, autoload_values=True)
	    targetmodel = ForeignKey(foreign_class=Tweet, compare_with='TimeUUIDType')

	class Following(Index):
	    username = RowKey(linked_from=User)
	    targetmodel = ForeignKey(foreign_class=User, compare_with='TimeUUIDType', 
	                             unique=True)    
    
	class FollowedBy(Index):
	    username = RowKey(linked_from=User)
	    targetmodel = ForeignKey(foreign_class=User, compare_with='TimeUUIDType',
	                             unique=True)

	# We're done with defining the API. Let's use it!

	dave = User(username='dave', firstname='dave', password='test').save()
	merlin = User(username='merlin', firstname='merlin', password='sunshine').save()
	peter = User(username='peter', firstname='Peter', password='secret').save()

	dave.follow(merlin, peter)
	peter.follow(merlin)
	merlin.follow(dave)

	merlin.tweet("i've just started using twitty. send me a message!")
	dave.tweet('making breakfast')
	peter.tweet('sitting at home being bored')

	for dude in (dave,peter,merlin):
	    name = dude['username']
	    print '%s has these followers:' % (name,), dude.get_followed_by().values()
	    print '%s follows' % (name,), dude.get_following().values()
	    print '%s sent' % (name,), [x for x in dude.get_tweets_sent(count=5)]
	    print '%s received' % (name,), [x for x in dude.get_tweets_received(count=5)]