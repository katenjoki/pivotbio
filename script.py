import pandas as pd
import glob
import os

AGRO_DATAPATH = "data/agronomist"
DW_DATAPATH ="data/data_warehouse/dw_data.csv"

'''the get_data function loops through all the csv files in our agronomist data path,melts each dataframe so as to create new 
columns 'variable' and 'value_agronomist' which contain the response variable and values per response variables respectively
It merges the 2 datasets and returns the relevant files stored in a csv file'''
def get_data(agro_datapath,dw_datapath):
    list_df =[]
    all_files = glob.glob(agro_datapath+ "/*.csv")
    for f in all_files:
        df_agro = pd.read_csv(f,sep=',')
        df_agro = df_agro.melt(id_vars=['Trial ID', 'reps', 'Plot No.', 'Rate Unit', 'Appl Timing'], var_name='variable', 
        value_name="value_agronomist")
        df_agro[['variable','sample__planned_growth_stage']] = df_agro.variable.str.split(" ",expand=True)
        list_df.append(df_agro)

    df = pd.concat(list_df,ignore_index=True)

    #loading the data warehouse csv file and renaming columns for uniformity
    dw = pd.read_csv("data/data_warehouse/dw_data.csv")
    dw.rename(columns={'experiment__trial_id':'Trial ID','plot__lookup_key':'Plot No.'},inplace=True)

    '''merging the 2 datasets, given that we want to compare the values from both 
    Ideally both datsets should be the same'''
    data = pd.merge(dw,df,how='left',left_on=['Trial ID','Plot No.','variable','sample__planned_growth_stage'],
    right_on=['Trial ID','Plot No.','variable','sample__planned_growth_stage'])

    #new dataframe with the relevant columns
    new_df = data.loc[:,['Trial ID','Plot No.','sample__planned_growth_stage','sample__items_per_sample','variable',
    'value', 'value_agronomist']]
    new_df.rename(columns={'Plot No.':'plot','sample__items_per_sample':'items_per_sample','value':'value_dw'},inplace=True)

    '''creating n_samples column which is number of samples taken for this plot, variable & growth stage combination according to the data warehouse'''
    plot = new_df['plot'].astype('str')
    new_df['combination'] = plot + "_" + new_df['variable'] + "_" +new_df['sample__planned_growth_stage'] 

    combinations = new_df.combination.value_counts()
    x = []
    for i in new_df.combination:
        for index, value in combinations.items():
            if i == index:
                x.append(value)

    new_df['n_samples'] = x
    new_df.drop(['combination'],axis=1,inplace=True)

    #save as csv file
    new_df.to_csv('data/clean_data.csv',index=False)

if __name__ == '__main__':
    get_data(AGRO_DATAPATH,DW_DATAPATH)   
