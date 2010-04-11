# Tragedy

Tragedy is a high-level Cassandra abstraction for Python.

## Tragedy's Data Model

To model your data in Tragedy, you define Models, Indexes and the relationships between them. A *Model* is a collection of data that can be accessed via a RowKey, and an *Index* is a 1-N mapping from one RowKey to one or more Models. A RowKey is just a unicode string. We usually use unique identifiers like usernames or email addresses for the RowKey. For Models that have no obvious unique identifier, we just let tragedy generate a UUID for us.

Here's an example:

	class Tweet(Model):
    	uuid    = RowKey(autogenerate=True) # generate a UUID for us.
    	message = StringField()    
    	author  = ForeignKey(foreign_class=User, mandatory=True)

Tweet is a Model, analogous to a Python class. If we instantiate Tweet, we get a specific tweet that we can write to the database:

    new_tweet = Tweet(message="Twittering from tragedy!", author='merlin')
	new_tweet.save()

Tweet instances are referred to and accessed by a RowKey, which in this case named `uuid` and is autogenerated on save. Object can only be retrieved from the datastore if their RowKey is known. Since Tweet's RowKey is random, we'll lose the Tweet if we don't keep a reference somehow. One way to do this, is to create an Index of all tweets a user posts:

	class TweetsSent(Index):
    	by_username = RowKey()
    	targetmodel = ForeignKey(foreign_class=Tweet, compare_with='TimeUUIDType')

	merlinIndex = TweetsSent(by_username='merlin')
	merlinIndex.append(new_tweet)
	merlinIndex.save()

TweetsSent is an abstract Index over Tweets, sorted by Cassandra's TimeUUIDType. merlinIndex is a specifc TweetsSent-Index for user 'merlin', as specified by the given RowKey during instantiation. Items can be added to an Index using the .append() method, and changes to them saved using the .save() method. If we lose track of an Index, we can, as long as we remember the RowKey of the index, load it again with the .load() method:

    tweets_by_user = TweetsSent(by_username='merlin').load()
	print tweets_by_user

The primary use of Indexes is to help us find Data whose RowKey we've forgotten  the call above gives us a list of Tweets all initialized with a RowKey of a Tweet previously posted by user 'merlin'. However the Tweets itself haven't been loaded yet, thus if we try to work with those tweets, we'd see #MISSING# fields:

    [<Tweet 8649a1ca4ab843b9afa6cc954908ac04: {'message': '#MISSING#', 'author': '#MISSING#'}, ...]

To actually load the tweets we need to resolve them. Luckily Indexes have the .resolve() helper to make this easy:

	tweets_by_user.resolve()
	print tweets_by_user
	[<Tweet ced314748d574379a817e1a1c9149789: {'message': "some message", 'author': <User merlin: {'password': '#MISSING#'}>}>

Behind the scenes Index.resolve() almost works like calling Model.load() on all Tweets in the list. It's more efficient though, since it combines all required queries into one multiquery for faster processing. Now we've seen how to create tweets, store them, and find them again. If you want to see how you can distribute them to Followers, scroll down for a full twitter demonstration below.

That's about it for the basics. There's more stuff Tragedy can do for you, and the following example shows of some of them, like automatic validation that Tragedy and Cassandra agree on the Data Model.

## Installation
  XXX setup.py soon XXX

## IRC
If you have questions catch me on IRC (enki on irc.freenode.net #cassandra), or send me an email to enki@bbq.io.

## Example (full twitter-demo)

    import tragedy
    client = tragedy.connect(['localhost:9160'])
    from tragedy import *
    
    twitty_cluster  = Cluster('Twitty Cluster')
    twitty_keyspace = Keyspace('Twitty', twitty_cluster)
    
    ALLTWEETS_KEY = '!ALLTWEETS!' # virtual user that receives all tweets
    
    class User(Model):
        """A Model is stored and retrieved by its RowKey.
           Every Model has exactly one RowKey and one or more other Fields"""
        username  = RowKey()
        firstname = StringField(mandatory=False)
        lastname  = StringField(mandatory=False) # normally fields are mandatory
        password  = StringField()
    
        def follow(self, *one_or_more):
            fol = Following(username=self)
            for other in one_or_more:
                fol.append(other)
                FollowedBy(username=other).append(self).save()
            fol.save()
    
        def tweet(self, message):
            new_tweet = Tweet(author=self, message=message[:140]).save()
            TweetsSent(by_username=self).append(new_tweet).save()
            
            tr = TweetsReceived(by_username=ALLTWEETS_KEY)
            tr.append(new_tweet).save()
            
            for follower in self.get_followed_by():
                follower.receive(new_tweet)            
    
        def receive(self, tweet):
            TweetsReceived(by_username=self).append(tweet).save()
    
        def get_followed_by(self, *args, **kwargs):
            return FollowedBy(username=self).load(*args, **kwargs)
    
        def get_following(self, *args, **kwargs):
            return Following(username=self).load(*args, **kwargs)
    
        def get_tweets_sent(self, *args, **kwargs):
            return TweetsSent(by_username=self).load(*args, **kwargs).resolve()
    
        def get_tweets_received(self, *args, **kwargs):
            return TweetsSent(by_username=self).load(*args, **kwargs).resolve()
    
    class Tweet(Model):
        uuid    = RowKey(autogenerate=True) # generate a UUID for us.
        message = StringField()    
        author  = ForeignKey(foreign_class=User, mandatory=True)
    
        @staticmethod
        def get_recent_tweets(*args, **kwargs):
            tr = TweetsReceived(by_username=ALLTWEETS_KEY)
            return tr.load(*args, **kwargs).loadIterValues()
    
        # def __repr__(self):
        #     return '<%s> %s' % (self['author']['username'], self['message'])
    
    class TweetsSent(Index):
        """An index is an ordered mapping from a RowKey to
           instances of a specific Model."""
        by_username = RowKey()
        targetmodel = ForeignKey(foreign_class=Tweet, compare_with='TimeUUIDType')
    
    class TweetsReceived(Index):
        by_username = RowKey()
        targetmodel = ForeignKey(foreign_class=Tweet, compare_with='TimeUUIDType')
    
    class Following(Index):
        username = RowKey()
        targetmodel = ForeignKey(foreign_class=User, compare_with='TimeUUIDType', 
                                 unique=True)    
    
    class FollowedBy(Index):
        username = RowKey()
        targetmodel = ForeignKey(foreign_class=User, compare_with='TimeUUIDType',
                                 unique=True)
    
    # We're done with defining the Data Model. Let's verify that Cassandra agrees on the model!
    twitty_keyspace.verify_datamodel()
    # Ok, all set. Let's go!
    
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
        print '%s sent' % (name,), [x for x in dude.get_tweets_sent(count=3)]
        print '%s received' % (name,), [x for x in dude.get_tweets_received(count=3)]