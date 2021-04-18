def wrapFun(func):
    def innner(*args,**kwargs):
        print(f'function name:{func.__name__}')
        rr=func(*args,**kwargs)
        return rr
    return inner


@wrapFun
def myadd(a,b):
    return a, b

dd=myadd(8,9)
print(dd)

