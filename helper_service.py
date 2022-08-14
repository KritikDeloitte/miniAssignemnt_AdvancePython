import re

# To convert csv data row to list 
def convert_csv_data_to_row_and_column(row):
    regex = re.compile(r"\\.|[\",]", re.DOTALL)
    delimiter = ''
    compos = [-1]
    for match in regex.finditer(row):
        g = match.group(0)
        if delimiter == '':
            if g == ',':
                compos.append(match.start())
            elif g in "\"":
                delimiter = g
        elif g == delimiter:
            delimiter = ''
       
    compos.append(len(row))
    return [ row[compos[i]+1:compos[i+1]] for i in range(len(compos)-1)]

def convert_csv_to_list():
    with open('movies.csv', 'r',errors='ignore') as f:
            results = []
            first = True
            for line in f:
                words = convert_csv_data_to_row_and_column(line)
                if first:
                    first = False
                    continue
                for i in [3,6,15,20]:
                    if(len(words[i])>0):
                        words[i]=int(words[i])
                if(len(words[21])>1):
                    words[i]=int(words[21][:-1])
                results.append(words)
            return results

def sort_budget_data(i):
    budget = i['budget'].strip(",.\"/")
    
    # del i['budget']
    if(budget.find('$')!=-1):
        budget=budget[1:].strip()
        while(budget.find(',')!=-1):
            index=budget.find(',')
            budget=budget[:index]+budget[index+1:]
        i['budget']= budget
        i['currency_type']= "USD"
    else:
        currency_name=budget[:3]
        budget=budget[4:].strip()
        i['budget']=budget
        i['currency_type']= currency_name

