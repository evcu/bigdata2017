from __future__ import print_function
import sys
import myUtils
import re,random 

def loadNYCtheaters():
    theaters = {}
    with open('theaters.csv','r') as f:
        f.next()
        for l in f: 
            aa = l.split(',')
            coor = map(float,filter(lambda x: len(x)>0,re.split('[^\d.-]+',aa[0])))
            name = aa[1]
            theaters[name] = coor

    return theaters

def createBox(c_tuple,x_lim,y_lim):
# tuple of floats c_tuple,new_point as (lon,lat)
    box = ((c_tuple[0]-x_lim,c_tuple[1]-y_lim),(c_tuple[0]+x_lim,c_tuple[1]+y_lim))
    def filterBox(dd):
        for i in range(len(dd)):
            if not(box[0][i]<dd[i]<box[1][i]):
                return False
        return True
    return filterBox

def filterTheBox(data,filterBox,c_lon,c_lat):
    p_lon = myUtils._fieldsDic[c_lon]
    p_lat = myUtils._fieldsDic[c_lat]
    filter_fun=lambda a: filterBox(map(float,[a[p_lon],a[p_lat]]))
    return data.filter(filter_fun)

def createUnionOfBoxes(c_tuples,x_lim,y_lim):
# tuple of floats c_tuple,new_point as (lon,lat)
    def genericfilterBox(dd,box):
        for i in range(len(dd)):
            if not(box[0][i]<dd[i]<box[1][i]):
                return False
        return True
    b_array = []
    for c_tuple in  c_tuples:
        b_array.append(((c_tuple[0]-x_lim,c_tuple[1]-y_lim),(c_tuple[0]+x_lim,c_tuple[1]+y_lim)))
    def unionFun(dd):
        for b in b_array:
            if genericfilterBox(dd,b):
                return True
        return False
    return unionFun

def getBarclaysEventDates(f_path='barclays_center_wiki_events.wiki'):
    lup = {'September':9,
         'October':10,
         'December':12,
         'February':2,
         'March':3,
         'November':11,
         'April':4,
         'May':5,
         'June':6,
         'July':7,
         'August':8,
         'January':1}
    starts=tuple(["| "+k for k in lup.keys()])
    dates = []
    with open(f_path,'rb') as f:
        for l in f:
            if l.startswith('==='):
                c_year = int(l.strip().split()[1])
            elif l.startswith(starts):
                a = l.strip().split()[1:3]
                a[1] = ''.join([c for c in a[1] if c.isdigit()])
                dt = (c_year,int(a[1]),lup[a[0]])
                dates.append(dt)
                print(dt)

    return dates