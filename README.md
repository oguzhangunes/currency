# currency
calculate currencies historically
#Features:
1. Can create historic currency data based on ANY currency and any date.
2. If object creation is a new object, it will create historic data with regarding dates.
3. If there is DB file with the parameter in the object creations, it will check missing dates and request the missing dates from API and insert to regarding DB table file
3. we can calculate average rate of currency for any time interval. If requested time is not in DB program will try find and insert them to DB from API
4. we can request last rate of currency, if it is not in DB file again program will understand it and try to get lastest data from API
5. while creating and object, if here will be no parameter. Default base currency is EUR and data will be for last 2 years. And this will take 3-5 minutes time
