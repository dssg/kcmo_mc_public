def tidy_name(name):
    #function to tidy up names (files/columns etc)
    name = str(name)
    name = name.lower()
    name = name.replace(" ", "_")
    name = name.replace("-", "_")
    name = name.replace("__","_")
    name = name.replace("___","_")
    name = name.replace(".csv", "")
    return(name)
