from flask import Flask, jsonify,request,abort
import pymssql
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from waitress import serve
a='SELECT AssetID,PointTemplateName,PointValue,Units '
b='FROM AssetMaster A '
c='inner join IBMSPoints P on P.AssetKey=A.AssetKey '
d='inner join IBMSPointLastValues L on L.PointKey=P.PointKey '
e='inner join IBMSPointTemplates T on T.PointTemplateKey=p.PointTemplateKey '
f='where AssetID like '
g='and PointTemplateName like '
query=a+b+c+d+e+f
password='130ls'
app = Flask(__name__)



limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)



@app.route('/asset/<asset>,<start>,<end>', methods=['GET'])
#@limiter.exempt
@limiter.limit("1 per second")
def get_task1(asset,start,end):
    pointlist=''
    if 'Authorization' in request.headers:
        token = request.headers['Authorization']
        if token!=password:
            abort(401)
            #return jsonify(error="401")
        else:
            conn = pymssql.connect(
            host=r'10.0.10.104',
            port=1433,
            user=r'WIN-4VTN20OQBVU\Administrator',
            password='Windows1!',
            database='opi',
            timeout=15
            )
            
            cur = conn.cursor()
            
            b='select pointkey,pointtemplatename from IBMSPoints am ' 
            c='inner join IBMSPointTemplates pt on pt.pointtemplatekey=am.pointtemplatekey '
            d='where assetkey='+str(asset)
            query=b+c+d
            c='IF OBJECT_ID(\'tempdb.dbo.#temp1\', \'U\') IS NOT NULL drop table #temp1 '

            #query1=query + '\'%'+str(asset)+'%\''
            #print(query)
            cur.execute(query)
            
            result = cur.fetchall()
            #print(result)
            thisdict = {}
            for i in result:
                #print(i)
                pointlist+=str(i[0])
                pointlist+=','
                thisdict[str(i[0])] =str(i[1])
            #print(thisdict)
                    
            #cur.close()
            conn.close()
            
            conn1 = pymssql.connect(
            host=r'10.0.10.105',
            port=1433,
            user=r'WIN-4VTN20OQBVU\Administrator',
            password='Windows1!',
            database='opi',
            timeout=15
            )
            
            cur1 = conn1.cursor()
            
            a='select PointKey,PointValue,TransactionDateTime FROM [IBMSTrends] '
            #b='inner join IBMSPoints P on P.pointkey=tr.pointkey inner join IBMSPointTemplates T on T.PointTemplateKey=p.PointTemplateKey '
            c1='where Pointkey in ('
            d1=') and transactiondatetime>\''
            e1='\' and transactiondatetime<\''
            f='\' order by transactiondatetime desc '
            
            query=a+c1+pointlist[:-1]+d1+start+e1+end+f
            #print(query)
            cur1.execute(query)
            result = cur1.fetchall()
            #print(result)
            thatdict={}
            final=[]
            for i in result:
                final.append({'pointname':thisdict[str(i[0])[:-2]],'pointvalue':i[1],'updatetime':str(i[2])})    
            #cur1.close()
            conn1.close()
            
            return jsonify({str(asset): final})
    else:
        abort(401)
        #return jsonify(error="401")
        


@app.route("/ping")
@limiter.exempt
def ping():
    return "PONG"

@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify(error="access denied bacause rate limit exceeded %s" % e.description)


if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=1234)
    app.run(  host='0.0.0.0',port=1234,debug=False,threaded=True)
    #http://127.0.0.1:5000/CS-B-MECH-L01-VAV-05-L1-01
