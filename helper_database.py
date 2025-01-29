########################################################################################################################
                                              #IMPORTS#
########################################################################################################################

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError, SQLAlchemyError
import pandas as pd
import time
import re

########################################################################################################################
                                           #ERROR HANDLING#
########################################################################################################################

class DatabaseError(Exception):
    """Classe de base pour toutes les exceptions liées aux bases de données."""
    pass

class ConnectionServerError(DatabaseError):
    """Exception levée lorsque la connexion au serveur échoue."""
    def __init__(self, reason = None, exception = None):
        if reason:
            super().__init__(f"Connection server error: {reason}")
        else:
            super().__init__("Connection server error without specific reason")
        self._exception = exception

class SqlQueryError(DatabaseError):
    """Exception levée lorsqu'une requête SQL échoue."""
    def __init__(self, reason=None, query=None, exception=None):
        message = f"Invalid SQL query: {reason}"
        if query:
            message += f" | Query: {query}"
        super().__init__(message)
        self._exception = exception
########################################################################################################################
                                             #DATABASE CLASS#
########################################################################################################################

class Database:
    def __init__(self, config:dict, name:str = ''):
        self._name = name.upper()

        if self._name not in config:
            raise ValueError(f"La base de données '{self._name}' n'existe pas.")

        self._server = config[self._name]
        self._connect_cmd = f'mssql+pyodbc://{self._server}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

        try:
            self._engine = create_engine(self._connect_cmd)

        except (OperationalError, ProgrammingError) as r:
            raise ConnectionServerError(exception = r)
        except SQLAlchemyError as r:
            raise ConnectionServerError(exception = r)
        except Exception as r:
            raise ConnectionServerError(f"Erreur inattendue lors de la connexion : {r}", exception = r)

        # Attribut à definir avec la méthode query
        self._query = None
        self._conn = None

    @property
    def name(self):
            return self._name

    @property
    def server(self):
        return self._server

    @property
    def connect_cmd(self):
        return self._connect_cmd

    @property
    def conn(self):
        """ Method pour checker l'état de la connexion"""
        if self._conn is None:
            return print('Aucune connexion active')
        pass

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, sql_cmd: str = None):
        """Permet de attribuer une requete sql donnée par l'utilisateur."""
        if not sql_cmd:
            raise SqlQueryError('Aucune commande SQL n’a été saisie')

        validation_query = f"SET NOEXEC ON; {sql_cmd}; SET NOEXEC OFF;"

        connector = self._engine.connect()
        try :
            connector.execute(validation_query)
            self._query = sql_cmd
            print(f"Syntaxe SQL valide!")

        except Exception as r:
            raise SqlQueryError(f"Requête SQL invalide, vérifiez la syntaxe", query=sql_cmd, exception=r)

        finally:
            connector.close()

    def __enter__(self):
        """Ouverture de la connexion à la base de données."""
        self._conn = self._engine.connect()
        print(f"Connexion à {self._name}\n")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fermeture automatique de la connexion."""
        self._conn.close()
        print(f"Deconnexion à {self._name}\n")

    def f_query(self, indent="    "):
        keywords = [
            r"\bSELECT\b", r"\bFROM\b", r"\bWHERE\b", r"\bAND\b", r"\bOR\b",
            r"\bBETWEEN\b", r"\bORDER BY\b", r"\bGROUP BY\b", r"\bHAVING\b",
            r"\bJOIN\b", r"\bLEFT JOIN\b", r"\bRIGHT JOIN\b", r"\bINNER JOIN\b",
            r"\bOUTER JOIN\b", r"\bINSERT INTO\b", r"\bVALUES\b", r"\bUPDATE\b",
            r"\bSET\b", r"\bDELETE\b"
        ]

        pattern = re.compile(r"(" + "|".join(keywords) + r")", re.IGNORECASE)

        # Remplacer avec un retour à la ligne et indentation
        formatted_query = pattern.sub(r"\n" + indent + r"\1", self._query)

        return formatted_query

    def execute_query(self, batch_size = 1000):
        if self._query is None:
            raise SqlQueryError("Aucune requête n'a été saisie")
        try:
            start_time = time.time()

            generator = pd.read_sql_query(self._query, self._conn, chunksize = batch_size)

            elapsed_time = time.time() - start_time
            print(f"Requête {self.f_query()}\nExécutée en {elapsed_time:.2f} secondes\n")
            return generator

        except Exception as r:
            raise SqlQueryError(reason = "Erreur lors de l'exécution de la requête",
                                query = self._query,
                                exception = r)
########################################################################################################################

if __name__ == '__main__':
    dbs1 = {'RMES':"sqlrmesprod\\rmesprod/rmes",'OPENDATA':"SQLOPENDATAPROD\\OPENDATAPROD/OPEN_DATA_PROD"}
    sql_query = "SELECT TOP 1000 * FROM [dbvw].[v_security_price]"

    gen_df = []

    with Database(config = dbs1, name = 'opendata') as opendata:
        opendata.query = sql_query
        for chk in opendata.execute_query():
            gen_df.append(chk)
    pass

    df = pd.concat(gen_df, ignore_index=True)
    print(sql_query, '\n')
    print(df.head(), '\n', df.shape,'\n')
    print(df.columns)