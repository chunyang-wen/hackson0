import random
import time
import sys
import string
import math

LEN_OBJ_ID = 32
LEN_HASH = 128
RW_FACTOR = 10


def rand_str(l: int) -> str:
    lower = string.ascii_lowercase
    upper = string.ascii_uppercase
    num = string.digits
    all = lower + upper + num
    return "".join(random.choices(all, k=l))


def rand_size() -> int:
    r = (1 - random.random()) * 0.99
    return math.ceil(math.log(r, 0.99))


if __name__ == '__main__':
    random.seed(time.time())
    n_objects = int(sys.argv[1])

    obj_set = set()
    while len(obj_set) != n_objects:
        for _ in range(n_objects - len(obj_set)):
            obj_set.add(rand_str(LEN_OBJ_ID))

    req_objs = list(obj_set) * RW_FACTOR
    random.shuffle(req_objs)

    for i, o in enumerate(req_objs):
        if o in obj_set:
            print(f"{i},W,{o},{rand_size()},{rand_str(LEN_HASH)}")
            obj_set.remove(o)
        else:
            print(f"{i},R,{o}")


