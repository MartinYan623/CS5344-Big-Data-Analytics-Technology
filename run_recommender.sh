#!/usr/bin/env bash
echo 'Start file 1'
python newtry.py --filename=/Users/lizhengyu/PycharmProjects/labassignment/user-based/clusters/allcity/allcity0.csv \
                 --type=item-based \
                 --similarity=pearson

echo 'Start file 2'
python newtry.py --filename=/Users/lizhengyu/PycharmProjects/labassignment/user-based/clusters/allcity/allcity1.csv \
                 --type=item-based \
                 --similarity=pearson

echo 'Start file 3'
python newtry.py --filename=/Users/lizhengyu/PycharmProjects/labassignment/user-based/clusters/allcity/allcity2.csv \
                 --type=item-based \
                 --similarity=pearson

echo 'Start file 4'
python newtry.py --filename=/Users/lizhengyu/PycharmProjects/labassignment/user-based/clusters/allcity/allcity3.csv \
                 --type=item-based \
                 --similarity=pearson