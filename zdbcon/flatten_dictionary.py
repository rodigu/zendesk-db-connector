
def flatten_dict(d: dict, key: str|list[str]) -> dict:
    """Recusively flattens a dictionary (dictionaries inside of dictionaries are also flattened).

    Lists with dictionaries are turned into dicts with keys using `key`.

    If `key` is a list, it attempts to use each item from the list in order as a key.

    :param dict d: dictionary to be flattened
    :param str | list[str] key: key or keys to be used in keying lists of dictionaries
    :return dict: flatenned dictionary
    """
    return { k: flatten_dict_list(v, key) for k, v in d.items() }

def flatten_dict_list(dlist: list[dict], key: str|list[str]) -> dict:
    """Flattens list of dictionaries recursively

    :param list[dict] dlist: list of dictionaries
    :param str | list[str] key: key(s) to be extrated from the dictionaries and turned into dict keys
    :return dict: flattened dictionary
    """
    wrong_type = lambda x: type(x)!=list and type(x)!=dict

    if wrong_type(dlist) or (type(dlist)==list and len(dlist) > 0 and wrong_type(dlist[0])):
        return dlist

    if type(dlist)==dict:
        return flatten_dict(dlist, key)

    def choose_key(d: dict) -> str:
        if type(key)==str: return key
        for k in key:
            if k in d: return k

    flattened = { d[choose_key(d)]: flatten_dict_list(d, key) for d in dlist }

    for v in flattened.values():
        v.pop(choose_key(v))

    return flattened
