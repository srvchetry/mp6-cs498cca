import json
import redis
import pymysql.cursors
import base64

endpoint = 'database-1.cfncxlbydguf.us-east-1.rds.amazonaws.com'
username = 'admin'
password = 'mp6schetry2'
database_name = 'mp6'
REDIS_URL ="mp6schetry2.alllvw.ng.0001.use1.cache.amazonaws.com"

connection = pymysql.connect(host= endpoint,
                             user= username,
                             password= password,
                             database= database_name,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

TTL = 1
 
Cache = redis.Redis(REDIS_URL)

def lambda_handler(event, context):
    
    
    cursor = connection.cursor()

    if (event['REQUEST'] == 'read'):
        
        result1 = []
        result2 = []
        get = "SELECT * FROM heroes WHERE id = %s"
        
        
        # lazy loading from cache first then from rds
        
        if(event['USE_CACHE'] == 'True'):
            
            for i in range(0,len(event['SQLS'])):
                
                id = event['SQLS'][i]
                
                res = Cache.get("SELECT * FROM heroes WHERE id = id")
                
                
                #return from cache if available
                
                if res:
                    
                    
                    
                    res = json.loads(res)
                    print(res)
                    for j in range(0,len(res)):
                        for k in range(0,len(res[j])):
                            row = res[j][k]
                            # print(row)
                            result1.append(row)
            

                    result = {
                        'statusCode': 200,
                        'body': result1
                        
                    }
                
                    return result
                
                #return from db if not available in cache, plus write to cache 
                
                
                
                cursor.execute(get,(id))
                res = cursor.fetchall()
                
                if res:
                    for l in range(0,len(res)):
                        row = res[l]
                    
                        result2.append(row)
                    
                    
                    
                    sql = "SELECT * FROM heroes WHERE id = id"
                    Cache.setex(sql,TTL, json.dumps(result2))
        
        
                    result = {
                        'statusCode': 200,
                        'body': result2
                        
                        }
                    
                    
            
                    return result
        
        return result
            
    else:
        
        getb4post = "Select name from heroes"
        cursor.execute(getb4post)
        result = cursor.fetchall()
        names = []
        
        for row in result:
            names.append(row['name'])
            

        post = "INSERT INTO heroes (hero, name, power, xp, color) VALUES (%s, %s, %s, %s, %s)"
        get_records = "SELECT COUNT(*) FROM heroes"
        next_id = "ALTER TABLE heroes AUTO_INCREMENT = %s"
        for i in range(0,len(event['SQLS'])):
            
            if event['SQLS'][i]['name'] not in names:
                cursor.execute(get_records)
                result = cursor.fetchall()
                count = result[0]['COUNT(*)']

                cursor.execute(next_id,(count+1))
                res = cursor.execute(post,(event['SQLS'][i]['hero'],event['SQLS'][i]['name'],event['SQLS'][i]['power'],event['SQLS'][i]['xp'],event['SQLS'][i]['color']))
                # print(res)
                
                if(event['USE_CACHE'] == 'True'):
                    sql = "SELECT * FROM heroes"
                    Cache.setex(sql, TTL, json.dumps(res))
                    # print("Written to Cache")
                    
        
        connection.commit()    
        
        
        
        result = {
            'statusCode': 200,
            'body': 'write success'
            
        }
        
        return result
        
        
    return {
        
        'body': json.dumps(result)
    }
