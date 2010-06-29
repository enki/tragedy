from tragedy import *

dev_cluster  = Cluster('Dev Cluster')
twitty_keyspace = Keyspace('Twitty', dev_cluster)

ALLTWEETS_KEY = '!ALLTWEETS!' # virtual user that receives all tweets

class User(Model):
    """A Model is stored and retrieved by its RowKey.
       Every Model has exactly one RowKey and one or more other Fields"""
    userid    = RowKeySpec(autogenerate=True)
    username  = AsciiField()
    firstname = UnicodeField(mandatory=False)
    lastname  = UnicodeField(mandatory=False) # normally fields are mandatory
    password  = UnicodeField()

    allusers = AllIndexField() # Index over all users
    by_lastname = SecondaryIndexField(lastname) # All users by lastname
    by_firstname = SecondaryIndexField(firstname) # and by firstname

    following = ManualIndexField(target_model='User') # ManualIndexes are not automatically
    followed_by = ManualIndexField(target_model='User') # updated and can contain subsets of the Items
    
    tweets_sent = ManualIndexField(target_model='Tweet')
    tweets_received = ManualIndexField(target_model='Tweet')

    def follow(self, *one_or_more):
        fol = self.following
        
        for other in one_or_more:
            fol.append(other)
            other.followed_by.append(self).save()
        
        fol.save()

    def tweet(self, message):
        new_tweet = Tweet(author=self, message=message[:140]).save()
        a = self.tweets_sent
        a.append( new_tweet ).save()

        for follower in self.followed_by.load():
            print 'SENDING TO FOLLOWER', follower.load()
            follower.receive(new_tweet)            

    def receive(self, tweet):
        # self.load()
        print 'RECEIVE', self.tweets_received.load()
        self.tweets_received.append(tweet).save()
        print self.tweets_received

class Tweet(Model):
    uuid    = RowKeySpec(autogenerate=True) # generate a UUID for us.
    message = UnicodeField()    
    author  = ForeignKeyField(foreign_class=User, mandatory=True)
    
    alltweets = AllIndexField()

twitty_keyspace.connect(servers=['localhost:9160'], auto_create_models=True, auto_drop_keyspace=True)

dave = User(username='dave', firstname='dave', password='test').save()
bood = User(username='dave', firstname='dave', lastname='Bood', password='super').save()

merlin = User(username='merlin', firstname='merlin', lastname='Bood', password='sunshine').save()
peter = User(username='peter', firstname='Peter', lastname='Johnson', password='secret').save()
dave.follow(merlin, peter)
peter.follow(merlin)
merlin.follow(dave)
dave.follow(peter)

merlin.tweet("i've just started using twitty. send me a message!")
dave.tweet('making breakfast')
peter.tweet('sitting at home being bored')

print 'A', User.allusers(), list(User.allusers().load().resolve())
print 'B', User.by_lastname('Bood'), list(User.by_lastname('Bood').load().resolve())
print 'C', dave.tweets_received.load(), list(dave.tweets_received.load().resolve())