

def split_address(msg):
    ret_ids = []
    for i, p in enumerate(msg):
        if p:
            ret_ids.append(p)
        else:
            break

    return (ret_ids, msg[i + 1:])

 