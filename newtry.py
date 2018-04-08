import graphlab as gl
import argparse
import time

def train_test(name, type, similarity):
    actions = gl.SFrame.read_csv(name)

    if type == 'user-based':
        print 'user-based'
        training_data, validation_data = \
            gl.recommender.util.random_split_by_user(actions, user_id='business_index', item_id='user_index',
                                                     item_test_proportion=0.2)

        start = time.time()
        m2 = gl.item_similarity_recommender.create(training_data, user_id='business_index', item_id='user_index',
                                               target="stars", similarity_type='pearson')
        end = time.time()

        print 'train time:' + str(end - start)

    elif type == 'item-based':
        print 'item-based'
        training_data, validation_data = \
            gl.recommender.util.random_split_by_user(actions, user_id='user_index', item_id='business_index',
                                                     item_test_proportion=0.2)

        start = time.time()
        m2 = gl.item_similarity_recommender.create(training_data, user_id='user_index', item_id='business_index',
                                                   target="stars", similarity_type=similarity)
        end = time.time()

        print 'train time:' + str(end - start)

    else:
        print 'No such method'
        return

    t1 = time.time()
    pred = m2.predict(validation_data)
    target = validation_data['stars']
    t2 = time.time()

    print 'test time:' + str(t2 - t1)

    t3 = time.time()
    rmse = gl.evaluation.rmse(target, pred)
    t4 = time.time()
    print 'eval time:' + str(t4 - t3)
    print 'RMSE:', rmse


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--filename', type=str, help='A file contains user_id, item_id and ratings')
    parser.add_argument('--type', default='user-based', choices=['user-based', 'item-based'],
                        type=str, help='Choose user-based or item-based')
    parser.add_argument('--similarity', default='pearson', choices=['pearson', 'cosine', 'jaccard'],
                        type=str, help='Choose similarity type')
    args = parser.parse_args()

    train_test(name=args.filename, type=args.type, similarity=args.similarity)