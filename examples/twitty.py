from tragedy import *

dev_cluster  = Cluster('Dev Cluster')
twitty_keyspace = Keyspace('Twitty', dev_cluster)

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
    _default_field = ForeignKey(foreign_class=Tweet, compare_with='TimeUUIDType')

class TweetsReceived(Index):
    by_username = RowKey()
    _default_field = ForeignKey(foreign_class=Tweet, compare_with='TimeUUIDType')

class Following(Index):
    username = RowKey()
    _default_field = ForeignKey(foreign_class=User, compare_with='TimeUUIDType', 
                             unique=True)    

class FollowedBy(Index):
    username = RowKey()
    _default_field = ForeignKey(foreign_class=User, compare_with='TimeUUIDType',
                             unique=True)

twitty_keyspace.connect(servers=['localhost:9160'], auto_create_models=True, auto_drop_keyspace=True)

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
