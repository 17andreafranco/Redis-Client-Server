from xmlrpc.client import Error, MultiCall, ServerProxy

api = ServerProxy('http://localhost:8001',allow_none=True)

multi = MultiCall(api)
multi.readAPI()
multi.applyAPI("Vols","Preu",'lambda val: "Si" if val > 300 else "No"')
multi.columnsAPI()
multi.groupByAPI("Vols")
multi.headAPI(2)
multi.isinAPI("Preu",200,300)
multi.itemsAPI()

try:
    for response in multi():
        for x in response:
            print(x+"\n")

    print("--------MIN--------")
    print(api.minAPI('Preu'))
    print("--------MAX--------")
    print(api.maxAPI('Preu'))
except Error as v:
    print("ERROR", v)
