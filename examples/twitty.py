from tragedy import *

dev_cluster  = Cluster('Dev Cluster')
twitty_keyspace = Keyspace('Twitty', dev_cluster)

ALLTWEETS_KEY = '!ALLTWEETS!' # virtual user that receives all tweets

class User(Model):
    """A Model is stored and retrieved by its RowKey.
       Every Model has exactly one RowKey and one or more other Fields"""
    userid    = RowKey(autogenerate=True)
    username  = AsciiField()
    firstname = UnicodeField(mandatory=False)
    lastname  = UnicodeField(mandatory=False) # normally fields are mandatory
    password  = UnicodeField()

    allusers = AllIndex()
    by_lastname = SecondaryIndex(lastname)
    by_firstname = SecondaryIndex(firstname)

    following = ObjectIndex(target_model='User')
    followed_by = ObjectIndex(target_model='User')
    
    tweets_sent = ObjectIndex(target_model='Tweet')
    tweets_received = ObjectIndex(target_model='Tweet')

    def follow(self, *one_or_more):
        fol = self.following()
        for other in one_or_more:
            fol.append(other)
            other.followed_by().append(self).save()
        fol.save()
        # print "FOLLOW", fol, self.followed_by(user=other).load()

    def tweet(self, message):
        new_tweet = Tweet(author=self, message=message[:140]).save()
        # print 'wtf', new_tweet
        a = self.tweets_sent()
        # print 'TWOP', a, a._row_key_name
        a.append( new_tweet ).save()
        # print 'FROB', a

        for follower in self.followed_by().load():
            # print 'FOLLOWER', follower
            follower.receive(new_tweet)            

    def receive(self, tweet):
        # print self, 'RECEIVING', tweet
        self.tweets_received().append(tweet).save()

class Tweet(Model):
    uuid    = RowKey(autogenerate=True) # generate a UUID for us.
    message = UnicodeField()    
    author  = ForeignKey(foreign_class=User, mandatory=True)
    
    alltweets = AllIndex()
    
twitty_keyspace.connect(servers=['localhost:9160'], auto_create_models=True, auto_drop_keyspace=True)

dave = User(username='dave', firstname='dave', password='test').save()

bood = User(username='dave', firstname='dave', lastname='Bood', password='super').save()

merlin = User(username='merlin', firstname='merlin', lastname='Bood', password='sunshine').save()
peter = User(username='peter', firstname='Peter', password='secret').save()

dave.follow(merlin, peter)
peter.follow(merlin)
merlin.follow(dave)

merlin.tweet("i've just started using twitty. send me a message!")
dave.tweet('making breakfast')
peter.tweet('sitting at home being bored')

print 'A', User.allusers(), list(User.allusers().load().resolve())
print 'B', User.by_lastname('Bood'), list(User.by_lastname('Bood').load().resolve())
print 'C', dave.tweets_received(), list(dave.tweets_received().load().resolve())

# 
# for dude in (dave,peter,merlin):
#     name = dude['username']
#     print '%s has these followers:' % (name,), dude.get_followed_by().values()
#     print '%s follows' % (name,), dude.get_following().values()
#     print '%s sent' % (name,), [x for x in dude.get_tweets_sent(count=3)]
#     print '%s received' % (name,), [x for x in dude.get_tweets_received(count=3)]
