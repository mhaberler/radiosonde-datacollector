# from: https://stackoverflow.com/a/62186294/2468365
import collections
crumbs = False
def flatten(dictionary, parent_key=False, separator='.'):
    """
    Turn a nested dictionary into a flattened dictionary
    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary
    """

    items = []
    for key, value in dictionary.items():
        if crumbs: print('checking:',key)
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, collections.MutableMapping):
            if crumbs: print(new_key,': dict found')
            if not value.items():
                if crumbs: print('Adding key-value pair:',new_key,None)
                items.append((new_key,None))
            else:
                items.extend(flatten(value, new_key, separator).items())
        elif isinstance(value, list):
            if crumbs: print(new_key,': list found')
            if len(value):
                for k, v in enumerate(value):
                    items.extend(flatten({str(k): v}, new_key).items())
            else:
                if crumbs: print('Adding key-value pair:',new_key,None)
                items.append((new_key,None))
        else:
            if crumbs: print('Adding key-value pair:',new_key,value)
            items.append((new_key, value))
    return dict(items)
