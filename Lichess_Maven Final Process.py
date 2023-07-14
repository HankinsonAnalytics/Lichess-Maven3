#!/usr/bin/env python
# coding: utf-8

# In[3]:


import time
import pandas as pd
import numpy as np
from io import BytesIO
#specialized imports
import re
import io
import os
import stockfish
from stockfish import Stockfish
import chess


# In[3]:


analysis_df= pd.read_csv(r'C:\Users\CDHan\Documents\Online+Chess+Games\simplified_df.csv')
print(analysis_df)


# In[7]:


#initializing two columns with my column headers
eval_columns = ['WhiteEvaluation1', 'BlackEvaluation1', 'WhiteEvaluation2', 'BlackEvaluation2', 
                'WhiteEvaluation3', 'BlackEvaluation3', 'WhiteEvaluation4', 'BlackEvaluation4', 
                'WhiteEvaluation5', 'BlackEvaluation5', 'WhiteEvaluation6', 'BlackEvaluation6', 
                'WhiteEvaluation7', 'BlackEvaluation7', 'WhiteEvaluation8', 'BlackEvaluation8', 
                'WhiteEvaluation9', 'BlackEvaluation9', 'WhiteEvaluation10', 'BlackEvaluation10']
Move_Columns = ["WhiteMove1", "BlackMove1", "WhiteMove2", "BlackMove2", "WhiteMove3", "BlackMove3",
                "WhiteMove4", "BlackMove4", "WhiteMove5", "BlackMove5", "WhiteMove6", "BlackMove6",
                "WhiteMove7", "BlackMove7", "WhiteMove8", "BlackMove8", "WhiteMove9", "BlackMove9",
                "WhiteMove10", "BlackMove10"]


# In[6]:


#finding the deltas so that I can code ruled for blunder detection.
for col in eval_columns:
    dif_col = col + "_difs"
    if col == 'WhiteEvaluation1':
        analysis_df[dif_col] = analysis_df[col] - 0
    else:
        analysis_df[dif_col] = analysis_df[col] - analysis_df[previous_col]
    previous_col = col


# In[34]:


print(analysis_df)


# In[15]:


"""defining and applying a function to re-split my moves column and overwrite the uci-converted moves 
so that my move columns can be used to join back on to the dataframe"""
def convert_moves(x):
    game_list = x.split(" ")
    moves = game_list[:20]
    return moves

analysis_df[Move_Columns] = analysis_df['moves'].apply(convert_moves).apply(pd.Series)



# In[20]:


#identifying the first blunder and mistake in each game and denoting that in a column.
dif_columns = [col for col in analysis_df.columns if 'difs' in col]
analysis_df['FirstMistake'] = None
analysis_df['FirstBlunder'] = None

for index, row in analysis_df.iterrows():
    for col in dif_columns:
        if row[col] >= 300 or row[col] <= -300:
            analysis_df.at[index, 'FirstBlunder'] = col
            break
    else:
        for col in dif_columns:
            if row[col] >= 100 and row[col] < 300:
                analysis_df.at[index, 'FirstBlunder'] = col
                break
            if row[col] <= -100 and row[col] > -300:
                analysis_df.at[index, 'FirstMistake'] = col
                break 


# In[31]:


#converting the column names I recorded in the previous block to the move column header name.
def process_column_name(column_name):
    try:
        column_name = column_name.replace("_difs", "")
        column_name = column_name.replace("Evaluation", "Move")
        return column_name
    except:
        return None
analysis_df['FirstMistake'] = analysis_df['FirstMistake'].apply(process_column_name)
analysis_df['FirstBlunder'] = analysis_df['FirstBlunder'].apply(process_column_name)


# In[33]:


#looking up the first move and first blunder based on the move column name recorded in the previous block.

analysis_df['FirstMistakeMove'] = None
analysis_df['FirstBlunderMove'] = None

for index, row in analysis_df.iterrows():
    move_prefix = '1. '

    if row['FirstMistake'] is not None:
        if 'Black' in row['FirstMistake']:
            move_prefix = f"{row['FirstMistake'].split('Move')[1]}. ... "
        else:
            move_prefix = f"{row['FirstMistake'].split('Move')[1]}. "

        move_san = row[row['FirstMistake']]
        analysis_df.at[index, 'FirstMistakeMove'] = f"{move_prefix}{move_san}"

    if row['FirstBlunder'] is not None:
        print(row['FirstBlunder'])  # Debug statement
        if 'Black' in row['FirstBlunder']:
            move_prefix = f"{row['FirstBlunder'].split('Move')[1]}. ... "
        else:
            move_prefix = f"{row['FirstBlunder'].split('Move')[1]}. "

        move_san = row[row['FirstBlunder']]
        analysis_df.at[index, 'FirstBlunderMove'] = f"{move_prefix}{move_san}"



# In[35]:


#pulling in my dataframe from part 1
join_df = pd.read_csv(r"C:\Users\CDHan\Documents\Online+Chess+Games\chess_games_processed.csv")
print(join_df)


# In[34]:


#recreating the move columns
join_df[Move_Columns] = join_df['moves'].apply(convert_moves).apply(pd.Series)


# In[42]:


#Dropping repeated columns and joing on to my main dataframe
analysis_df = analysis_df.drop(columns=["game_id", "moves"])
join_df = join_df.merge(analysis_df, how='left', on=Move_Columns)


# In[43]:


#saving to CSV for backup
join_df.to_csv(r'C:\Users\CDHan\Documents\Online+Chess+Games\Analyzed_Games.csv', index=False)


# In[4]:


#Pulling in the CSV to create new columns it
touchup_df= pd.read_csv(r'C:\Users\CDHan\Documents\Online+Chess+Games\Analyzed_Games.csv')
print(touchup_df.columns)


# In[45]:


#pulling my difs column names into a list for each color.
white_difs_list = [col for col in touchup_df.columns if 'White' in col and 'difs' in col]
black_difs_list = [col for col in touchup_df.columns if 'Black' in col and 'difs' in col]
print(white_difs_list)


# In[47]:


#creating columns to count blunders
touchup_df['white_blunders_count'] = np.count_nonzero((touchup_df[white_difs_list] <=-300),axis=1)
touchup_df['black_blunders_count'] = np.count_nonzero((touchup_df[black_difs_list] >=300) , axis=1)
touchup_df['white_mistakes_count'] = np.count_nonzero(np.logical_and(touchup_df[white_difs_list] > -300, touchup_df[white_difs_list] < -100), axis=1)
touchup_df['black_mistakes_count'] = np.count_nonzero(np.logical_and(touchup_df[black_difs_list] >= 100, touchup_df[black_difs_list] < 300), axis=1)
touchup_df['Total Mistakes'] = touchup_df['white_mistakes_count'] + touchup_df['black_mistakes_count']
touchup_df['Total Blunders'] = touchup_df['white_blunders_count'] + touchup_df['black_blunders_count']


# In[29]:


"""Creating a column that will track the line up to the first blunder which will serve to help isolate the blunder from
other moves in other positions (ex. if Bb5 is a blunder in two unique positions, the game moves will distinguish the positons
from one another so that they are not counted as the same move as such)"""
touchup_df['First Blunder Moves'] = None

for index, row in touchup_df.iterrows():
    if pd.isnull(row['FirstBlunder']):
        touchup_df.at[index, 'First Blunder Moves'] = None       
    else:
        target_move = row['FirstBlunder']
        working_columns = touchup_df.columns[:touchup_df.columns.get_loc(target_move)+1]
        move_count = 0
        moves_formatted = ""
        for move in working_columns:
            if "White" in move:
                move_count += 1
                move_number = str(move_count) + ". "
                moves_formatted += move_number + row[move]
            elif "Black" in move:
                moves_formatted += " " + row[move] + " "
        touchup_df.at[index, 'First Blunder Moves'] = moves_formatted

        


# In[ ]:


print(touchup_df[['FirstBlunder', 'FirstBlunderMove','First Blunder Moves']]) #Viewing the result


# In[33]:


touchup_df.to_csv(r'C:\Users\CDHan\Documents\Online+Chess+Games\Analyzed_Games.csv', index=False)#saving to the final CSV

