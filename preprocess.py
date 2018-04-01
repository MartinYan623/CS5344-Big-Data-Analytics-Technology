import csv
import time
import pandas as pd

def way1():
    start = time.time()
    with open('yelp_tip.csv', 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        user = [row['user_id'] for row in reader]
        business = [row['business_id'] for row in reader]

    finish = time.time()
    print len(user)
    print str(finish-start)+'s'
    csvfile.close()

def read_user():
    """1326100
       44.9781861305s"""
    start = time.time()
    colums = ['user_id', 'name', 'review_count', "yelping_since", "friends", "useful", "funny", "cool", "fans", "elite",
              "average_stars", "compliment_hot", "compliment_more", "compliment_profile",  "compliment_cute",
              "compliment_list", "compliment_note", "compliment_plain", "compliment_cool", "compliment_funny",
              "compliment_writer", "compliment_photos"]
    d = pd.read_csv('yelp_user.csv', usecols=colums)
    user_id = d['user_id'].tolist()
    d.to_csv('user_friend.csv', columns=['user_id', 'friends'], header=True, index=False)

    # friends_id = d['friends'].tolist()
    # h5 = pd.HDFStore('user.h5', 'w')
    # h5['user_id'] = user_id
    # h5['friends'] = friends_id
    # h5.close()
    finish = time.time()

    print len(user_id)
    print str(finish-start)+'s'

def read_review():
    """5261668
       61.6312339306s"""
    start = time.time()
    colums = ['review_id', 'user_id', 'business_id', "stars", "date", "text", "useful", "funny", "cool"]
    d = pd.read_csv('yelp_review.csv', usecols=colums)
    user_id = d['user_id'].tolist()
    d.to_csv('review.csv', columns=['user_id', 'business_id', 'stars'], header=True, index=False)
    # friends_id = d['friends'].tolist()
    # h5 = pd.HDFStore('user.h5', 'w')
    # h5['user_id'] = user_id
    # h5['friends'] = friends_id
    # h5.close()
    finish = time.time()

    print len(user_id)
    print str(finish-start)+'s'

def id_to_num(num):
    '''read data'''
    colums = ['user_id', 'business_id', "stars"]
    df = pd.read_csv('review.csv', usecols=colums)

    user_id = df['user_id'].tolist()
    business_id = df['business_id'].tolist()

    n_users = df.user_id.unique().tolist()
    n_business = df.business_id.unique().tolist()

    user_index = []
    business_index = []
    i = 0
    print "start to index user"
    for user in user_id[num*500000:(num+1)*500000]:
        u_num = n_users.index(user)
        user_index.append(u_num)
        if i % 1000 == 0:
            print 'finished %d' % i
        i += 1

    print 'start to index business'

    j = 0
    for item in business_id[num*500000:(num+1)*500000]:
        b_num = n_business.index(item)
        business_index.append(b_num)
        if j % 1000 == 0:
            print 'finished %d' % j
        j += 1

    print 'srtat to save %d' % num
    star = df['stars'].tolist()
    dataframe = pd.DataFrame({'user_index': user_index, 'business_index': business_index, 'stars': star[num*500000:(num+1)*500000]})

    dataframe.to_csv('review_%d.csv' % num, header=True, index=False, sep=',')

if __name__ == '__main__':

    id_to_num(1)
    id_to_num(2)
    id_to_num(3)
    id_to_num(4)
    id_to_num(5)
    id_to_num(6)
    id_to_num(7)
    id_to_num(8)
    id_to_num(9)