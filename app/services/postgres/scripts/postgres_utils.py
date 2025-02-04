
# this functions expect the results of postgres and returns in proper format
def exec_one(res):
    return [list(i)[0] for i in res]

# this function takes in the variable names that we want and then gives us the object



def exec_all(res, cols = []):
    result = []
    for i in res:
        i = list(i)
        obj = {}
        for j in range(0, len(cols)):
            obj[cols[j]] = i[j]
        result.append(obj)
    return result