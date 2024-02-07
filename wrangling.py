
def handling_accuracy(data):
    # Code for handling accuracy
    # formats and types
    
    return data

def handling_completeness(data):
    # Code for handling completeness
    # missing columns
    # imputations
    
    return data

def handling_integrity(data):
    # Code for handling integrity
    # business rules
    return data

def handling_text(data):
    # Code for handling text
    # harmonisation,business rules, deduplication
    return data

def data_protection(data):
    # Code for data protection
    # anonymisation,data retention
    return data


def wrangling(data):
    data = handling_accuracy(data)
    data = handling_completeness(data)
    data = handling_integrity(data)
    data = handling_text(data)
    data = data_protection(data)
    
    return data

if __name__ == "__main__":
    data = None
    wrangling(data)
