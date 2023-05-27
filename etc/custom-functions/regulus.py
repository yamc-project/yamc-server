# Regulus utility functions 

def hp_target(v1, v2):
    """
    Returns heat pump target value based on value of v1 or v2.
    Values v1 and v2 are retrieved from 3_SCH.XML document and correspond to 
    //INPUT[43]/@VALUE and //INPUT[19]/@VALUE respectively. 
    This was reverse engineered from the regulus app version IR14CTC v1.0.3.0, 10.11.2021 
    """
    if v1==1:
        return 'washwater'
    elif v2==1:
        return "heating"
    else:
        return 'None'
