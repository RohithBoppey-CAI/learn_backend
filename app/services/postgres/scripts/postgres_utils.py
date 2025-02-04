
# this functions expect the results of postgres and returns in proper format
def exec_one(res):
    return [list(i)[0] for i in res]

# this function takes in the variable names that we want and then gives us the object
def exec_all(res):
    return [{}]