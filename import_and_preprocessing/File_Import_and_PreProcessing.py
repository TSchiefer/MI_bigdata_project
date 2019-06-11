# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 21:18:57 2019

@author: wb

File Import and Pre-Processing

"""

# %% general stuff
import os
import pandas as pd


# %% file import

# read the file line by line
#FileName = 'amazom-meta_small.txt'
FileName = 'amazon-meta.txt'
FilePath = 'D:\\10-ZhAW\\3-MachineIntelligence\\4-BigData\\30_Projekt'

FullPath = os.path.join(FilePath, FileName)

with open(FullPath, encoding="utf8") as fp:
    content = fp.readlines()
    content = content[6:]           # skip first 6 lines


#%% process content line by line
id_no = []
ASIN = []    
Group = []
Salesrank = []
Title = []
Reviews = []
avg_rating = []
src = []
dest = []

metadata_flag = 0

for line in content:
    
    # check for top level product info
    if not(metadata_flag):
        
        if 'Id:' in line:
            temp = line.split('Id: ')[1].strip(' ').strip('\n')
            id_no_temp = int(temp)
            try:
                id_no.append(id_no_temp)
            except:
                print('Unable to store id-no. Perhaps wrong pattern detected')
            continue
                        
        if 'ASIN' in line and not('cutomer' in line):
            temp = line.split(' ')[1].strip(' ').strip('\n')
            ASIN.append(temp)
#            new_asin = 1
            metadata_flag = 1
    #        print('found new ASIN: {}'.format(temp))
            continue
         
        
    #print('checking for new metadata: {}'.format(metadata_flag))
    if metadata_flag:
        # new review detected
        # check for further meta-data, set values to -1 if not existing
        #print('checking meta data')
        
        if 'title' in line:
            t_t = line.split('title: ')[1].strip('\n')
            Title.append(t_t)
            continue
        
        if 'group' in line and not('Supergroups' in line):
            try:
                g_t = line.split(': ')[1].strip('\n')
            except:
                print(line)   
            Group.append(g_t)
            continue
        
        if 'salesrank' in line:
            sr_t = line.split(': ')[1].strip('\n')
            if (int(sr_t) <= 0) or (int(sr_t) > 5000):
                # skip this product and remove last ASIN and id
                metadata_flag = 0
                del id_no[-1]
                del ASIN[-1]
                del Title[-1]
                del Group[-1]
                continue
            Salesrank.append(int(sr_t))
            continue
        
        if 'similar: ' in line:           
            line_temp = line.split('similar: ') 
            sim_t = line_temp[1].strip('\n').split('  ')    
            no_sim = int(sim_t[0])

            src_temp = [ASIN[-1]]*no_sim
            dest_temp = sim_t[1:]
            src.extend(src_temp)
            dest.extend(dest_temp)
            continue
            
        if 'reviews: ' in line:
#            r_t = line.split('reviews: total: ')[1].split(' ')[0]
            line_split = line.split('avg rating: ')
            line_p1 = line_split[0]
            line_p2 = line_split[1]
            r_t = line_p1.split('reviews: total: ')[1].split(' ')[0]
            ar_t = line_p2.strip('\n')
            Reviews.append(int(r_t))
            avg_rating.append(float(ar_t))
            metadata_flag = 0



# %% create dataframe and export results

data_dict = {'Id': id_no, 'ASIN': ASIN, 'Group': Group, 'Salesrank': Salesrank,
      'Reviews': Reviews, 'avg rating': avg_rating, 'Title': Title}

df = pd.DataFrame(data = data_dict)

#df.to_csv('ProductData_small.csv', sep=';', header=True, index=False)


       
link_dict = {'src': src, 'dest': dest}
df_link = pd.DataFrame(data = link_dict)

#df_link.to_csv('LinkData_small.csv', sep=';', header=True, index=False)





