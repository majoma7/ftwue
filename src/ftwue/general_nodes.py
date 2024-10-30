def retrieve_dataset(*args): # need placeholder args
    # This node doesn't perform any operation.
    # It ensures that reading happens after writing.
    flag = args[-1]
    return tuple(args[:-1])