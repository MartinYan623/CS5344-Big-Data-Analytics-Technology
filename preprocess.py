import csv
import time
import pandas as pd


def id_to_num(num):
    '''read data'''
    colums = ['user_id', 'business_id', "stars"]
    df = pd.read_csv('review_old.csv', usecols=colums)

    user_id = df['user_id'].tolist()
    business_id = df['business_id'].tolist()

    n_users = df.user_id.unique().tolist()
    n_business = df.business_id.unique().tolist()

    user_index = []
    business_index = []
    i = 0
    pre_user = '0'
    pre_index = 0
    print ("start to index user")
    for user in user_id[num*500000:5261668]:
        if user == pre_user:
            user_index.append(pre_index)
        else:
            u_num = n_users.index(user)
            user_index.append(u_num)
            pre_user = user
            pre_index = u_num+0

        if i % 1000 == 0:
            print ('finished %d' % i)
        i += 1

    print ('start to index business')

    j = 0
    pre_bus = '0'
    pre_busi = 0
    for item in business_id[num*500000:5261668]:
        if item == pre_bus:
            business_index.append(pre_busi)
        else:
            b_num = n_business.index(item)
            business_index.append(b_num)
            pre_bus = item
            pre_busi = b_num + 0

        if j % 1000 == 0:
            print ('finished %d' % j)
        j += 1

    print ('srtat to save %d' % num)
    star = df['stars'].tolist()
    dataframe = pd.DataFrame({'user_index': user_index, 'business_index': business_index, 'stars': star[num*500000:5261668]})

    dataframe.to_csv('review_%d.csv' % num, header=True, index=False, sep=',')

if __name__ == '__main__':

    id_to_num(10)
