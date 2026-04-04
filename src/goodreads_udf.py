from pyspark.sql.types import StringType,TimestampType
from pyspark.sql.functions import udf
from time import strptime
from datetime import datetime

# A Simple UDF to remove extra spaces from the text columns
remove_extra_spaces = udf(lambda x: ' '.join(x.split()),StringType())
@udf(TimestampType())
def stringtodatetime(x):
    """
    A simple UDF to convert string to datetime format. The date format in the source dataset is inconsistent, so this UDF tries to parse the date in multiple formats and convert it to a consistent format.
    :param x: input string
    :return: datetime object
    """
    x =  datestring.split()
    day,month,year = int(x[2]),strptime(x[1],'%b').tm_mon,int(x[5])
    hour,minute,second = [int(val) for val in x[3].split(':')]
    return datetime(year=year,month=month,day=day,hour=hour,minute=minute,second=second)
