from django.shortcuts import render
from django.db import connections
from django.http import JsonResponse ,HttpResponseRedirect

cursor = connections['default'].cursor()
def fetch_comp_data(request):
    try:
        #query to get all companies data
        cursor.execute("SELECT * FROM company")
        data=cursor.fetchall()
        dict_data=dict(x for x  in data)
        return JsonResponse(dict_data)
    except Exception:
        return JsonResponse({"status":"unsuccessful"})


def fetch_esg_scores(request, riccode):
    try:
        #query to find name of the company whit this specific riccode
        cursor.execute("SELECT companyname FROM company WHERE riccode=%s",[riccode])
        cname=cursor.fetchone()
        #query to find esgscores for the company with specified name
        cursor.execute("SELECT * FROM esgscores WHERE companyname=%s",[cname[0]])
        data=cursor.fetchone()
        #convert datas in a tuple to datas in a dictionary
        dict_data={"companyname":data[0],"environment":data[1],"social":data[2],"governance":data[3],"rankings":data[4],"totalindustries":data[5],"totalscore":data[6]}
        return JsonResponse(dict_data)
    except Exception:
        return JsonResponse({"status":"unsuccessful"})