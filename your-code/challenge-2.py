# import libraries
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import re

class Connector:
    def __init__(self, connection_path):
        self.engine = create_engine(connection_path)
        self.conn = self.engine.connect()
    
    def read_sql(self, sql):
        return pd.read_sql(sql, self.conn)

    def to_sql(self, df, table_name, ifexists):
        df.to_sql(table_name, self.conn, if_exists=ifexists)

    def __del__(self):
        print('-----------------------')
        try:
            self.conn.close()
        except:
            print('could not close connection')
        try:
            self.engine.dispose()
        except:
            print('could not close engine')
        finally:
            print('Connector destroyed.')


driver   = 'mysql+pymysql:'
user     = 'root'
password = '53cawentOo69'
ip       = 'localhost'
database = 'pokemon'
local = Connector(f'{driver}//{user}:{password}@{ip}/{database}')
pokemon = local.read_sql('SELECT * FROM pokemon_stats')

# CHALLENGE 1

print(pokemon.columns)
pokemon['Test_Total'] = pokemon[['HP', 'Attack', 'Defense', 'Sp. Atk', 'Sp. Def', 'Speed']].sum(axis=1)
print(pokemon.nlargest(10, 'Total'))
print('Does the Test_Total equals to real Total column? ', pokemon.Total.equals(pokemon.Test_Total))
print('As long as total is the sum of the 6 parameters, we assume the highest total is the one with most stats')


# CHALLENGE 2

print(pokemon.shape)
types_1 = pd.get_dummies(pokemon['Type 1'])
types_2 = pd.get_dummies(pokemon['Type 2'])
types = types_1 + types_2
pokemon = pd.concat([pokemon, types], axis=1)
print(pokemon)


# CHALLENGE 3

columns = list(types.columns)
columns.insert(0, 'Total')
corr_pokemons = pokemon[columns].corr()
corr_pokemons = corr_pokemons.drop(['Total'], axis=0)
print(corr_pokemons)


# CHALLENGE 4

print(corr_pokemons.sort_values('Total', ascending=False).head(2))