import MySQLdb
import time
# Open database connection
db = MySQLdb.connect("cloud2.chjzzgbk6tug.us-west-2.rds.amazonaws.com","lavanya","beauty$$14","clouddata" )

# prepare a cursor object using cursor() method
cursor = db.cursor()

# Drop table if it already exist using execute() method.
#cursor.execute("")
begin = time.time()
# Create table as per requirement
sql = """select 
    week, count(Mag2to3) as Mag2to3, count(Mag3to4) as Mag3to4, count(Mag4to5) as Mag4to5, count(MagAbove5) as MagAbove5
from
    ((select 
        case
                when date(quake_time) between cast('2015-01-20' as date) and cast('2015-01-26' as date) then 1
                when date(quake_time) between cast('2015-01-27' as date) and cast('2015-02-02' as date) then 2
                when date(quake_time) between cast('2015-02-03' as date) and cast('2015-02-09' as date) then 3
                when date(quake_time) between cast('2015-02-10' as date) and cast('2015-02-16' as date) then 4
                when date(quake_time) between cast('2015-02-17' as date) and cast('2015-02-23' as date) then 5
                when date(quake_time) between cast('2015-02-24' as date) and cast('2015-02-29' as date) then 6
            end week,
            earthquake.id
    from
        earthquake) as week, (select 
        case
                when mag between 2 and 2.99 then mag
            end Mag2to3,
            earthquake.id
    from
        earthquake) as Mag2to3, (select 
        case
                when mag between 3 and 3.99 then mag
            end Mag3to4,
            earthquake.id
    from
        earthquake) as Mag3to4, (select 
        case
                when mag between 4 and 4.99 then mag
            end Mag4to5,
            earthquake.id
    from
        earthquake) as Mag4to5, (select 
        case
                when mag >= 5 then mag
            end MagAbove5,
            earthquake.id
    from
        earthquake) as MagAbove5)
where
    week.id = Mag2to3.id and
    week.id = Mag3to4.id and
    week.id = Mag4to5.id and
    week.id = MagAbove5.id 
group by week;"""

cursor.execute(sql)
print 'Success!'
end = time.time()
mytime = end - begin
print 'Time taken: ', mytime , 'Seconds'

for i in range(1-2000):
begin = time.time()
cursor = db.cursor()
randquery = """select * from (select * from earthquake limit 2000) A group by rand() limit 10;"""
cursor.executequery(randquery)
end = time.time()
timetoquery = end - begin
print 'Time taken for 2000 queries: ', timetoquery , 'Seconds'


# disconnect from server
db.close()