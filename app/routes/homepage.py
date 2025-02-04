from fastapi import APIRouter

import os

# make a new router instance in here
# if more than one tag is given in here, then we are getting the same API requests inside the Swagger Docs
tags = []
tags.append('Home Router!!')

# uncomment this to see more than one tags
# tags.append('First Router!!')

homepage_router = APIRouter(prefix='/home', tags=tags)

@homepage_router.get('/function_a')
def test_home_router():
    return {'status': 'You are testing the router link, am I right?? 🤔'}


@homepage_router.get('/function_b')
def test_home_router():
    return {'status': 'You are testing the router link, am I right?? 🤔'}


@homepage_router.get('/function_c')
def test_home_router():
    return {'status': 'This is the first output🤔'}


@homepage_router.get('/function_c')
def test_home_router():
    return {'status': 'This is the second output🤔'}
