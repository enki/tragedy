from tragedy import *

dev_cluster  = Cluster('Dev Cluster')
twitty_keyspace = Keyspace('Twitty', dev_cluster)

class User(Model):
    """A Model is stored and retrieved by its RowKey.
       Every Model has exactly one RowKey and one or more other Fields"""
    userid    = RowKey(autogenerate=True)
    username  = AsciiField()
    firstname = UnicodeField(mandatory=False)
    lastname  = UnicodeField(mandatory=False) # normally fields are mandatory
    password  = UnicodeField()

class Tweet(Model):
    uuid    = RowKey(autogenerate=True) # generate a UUID for us.
    message = UnicodeField()    
    author  = ForeignKey(foreign_class=User, mandatory=True)

class TweetsSent(Index):
	by_username = RowKey()
	targetmodel = ForeignKey(foreign_class=Tweet, compare_with='TimeUUIDType')

def run():
    # Connect to cassandra
    twitty_keyspace.connect(servers=['localhost:9160'], auto_create_models=True, auto_drop_keyspace=True)

    dave = User(username='dave', firstname='dave', password='test').save()
    merlin = User(username='merlin', firstname='merlin', lastname='Bood', password='sunshine').save()
    peter = User(username='peter', firstname='Peter', password='secret').save()

    new_tweet = Tweet(author=dave, message='tweeting from tragedy').save()
    merlinIndex = TweetsSent(by_username=merlin['username'])
    merlinIndex.append(new_tweet)
    merlinIndex.save()
    
    tweets_by_user = TweetsSent(by_username='merlin').load()
    print tweets_by_user
    print list(tweets_by_user.resolve())

if __name__ == '__main__':
    run()